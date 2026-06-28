package gr.ntua.dsml.q1;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static gr.ntua.dsml.TestSpark.*;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Q1: the DataFrame, DataFrame+UDF and RDD implementations must produce the
 * same per-part [street, total] counts on a hand-checked fixture.
 *
 * Fixture (premis, time_occ) -> expected: Morning[1,2] Afternoon[1,1]
 * Evening[0,1] Night[2,2].
 */
class Q1Test {

    private static Dataset<Row> crimeFixture() {
        return df(List.of(
                        row("STREET", 600),     // Morning, street
                        row("STREET", 1300),    // Afternoon, street
                        row("SIDEWALK", 700),   // Morning, not street
                        row("STREET", 2200),    // Night, street
                        row("STREET", 100),     // Night, street
                        row("APARTMENT", 1800)),// Evening, not street
                str("premis"), intf("time_occ"));
    }

    private static void assertExpected(Map<String, long[]> m) {
        assertArrayEquals(new long[]{1, 2}, m.get("Morning"));
        assertArrayEquals(new long[]{1, 1}, m.get("Afternoon"));
        assertArrayEquals(new long[]{0, 1}, m.get("Evening"));
        assertArrayEquals(new long[]{2, 2}, m.get("Night"));
    }

    private static Map<String, long[]> fromDf(Dataset<Row> agg) {
        Map<String, long[]> m = new HashMap<>();
        for (Row r : agg.collectAsList()) {
            m.put(r.getString(r.fieldIndex("part")),
                    new long[]{r.getLong(r.fieldIndex("street")), r.getLong(r.fieldIndex("total"))});
        }
        return m;
    }

    @Test
    void dataFrame() {
        assertExpected(fromDf(Q1Df.compute(crimeFixture())));
    }

    @Test
    void dataFrameUdf() {
        assertExpected(fromDf(Q1DfUdf.compute(get(), crimeFixture())));
    }

    @Test
    void rdd() {
        JavaPairRDD<String, long[]> agg =
                Q1Rdd.compute(crimeFixture().select(col("premis"), col("time_occ")).javaRDD());
        assertExpected(new HashMap<>(agg.collectAsMap()));
    }

    // - Null TIME OCC: must be dropped identically by all three impls so they
    //     stay in agreement (df filters isNotNull, rdd filters isNullAt(1)). -

    private static Dataset<Row> crimeWithNullTime() {
        return df(List.of(
                        row("STREET", 600),     // Morning, street
                        row("STREET", null),    // null time -> dropped everywhere
                        row("APARTMENT", 700)), // Morning, not street
                str("premis"), intf("time_occ"));
    }

    /** Only Morning survives, with [street=1, total=2]; the null row is gone. */
    private static void assertNullTimeDropped(Map<String, long[]> m) {
        assertEquals(1, m.size(), "null-time row must not create a part");
        assertArrayEquals(new long[]{1, 2}, m.get("Morning"));
    }

    @Test
    void nullTimeDroppedDataFrame() {
        assertNullTimeDropped(fromDf(Q1Df.compute(crimeWithNullTime())));
    }

    @Test
    void nullTimeDroppedDataFrameUdf() {
        assertNullTimeDropped(fromDf(Q1DfUdf.compute(get(), crimeWithNullTime())));
    }

    @Test
    void nullTimeDroppedRdd() {
        JavaPairRDD<String, long[]> agg = Q1Rdd.compute(
                crimeWithNullTime().select(col("premis"), col("time_occ")).javaRDD());
        assertNullTimeDropped(new HashMap<>(agg.collectAsMap()));
    }
}
