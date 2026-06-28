package gr.ntua.dsml.q3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.List;

import static gr.ntua.dsml.TestSpark.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Q3: DataFrame and RDD must compute the same per-capita income per ZIP and the
 * same descending order.
 *
 * Census blocks (zip,pop,households) aggregate to: 90001 -> (150,60),
 * 90002 -> (200,50), 90003 -> (0,0) dropped. Income joins 90001,90002 (90004
 * has no census). per_capita = income*households/pop:
 *   90001 = 60000*60/150 = 24000 ; 90002 = 80000*50/200 = 20000.
 */
class Q3Test {

    private static Dataset<Row> censusFixture() {
        return df(List.of(
                        row("90001", 100, 40), row("90001", 50, 20),
                        row("90002", 200, 50),
                        row("90003", 0, 0)),
                str("zip"), intf("pop"), intf("households"));
    }

    private static Dataset<Row> incomeFixture() {
        return df(List.of(
                        row("90001", 60000), row("90002", 80000), row("90004", 50000)),
                str("zip"), intf("income"));
    }

    @Test
    void dataFrame() {
        List<Row> r = Q3Df.compute(censusFixture(), incomeFixture(), "default").collectAsList();
        assertEquals(2, r.size());
        assertEquals("90001", r.get(0).getString(r.get(0).fieldIndex("zip")));
        assertEquals(24000.0, r.get(0).getDouble(r.get(0).fieldIndex("per_capita_income")), 1e-6);
        assertEquals("90002", r.get(1).getString(r.get(1).fieldIndex("zip")));
        assertEquals(20000.0, r.get(1).getDouble(r.get(1).fieldIndex("per_capita_income")), 1e-6);
    }

    @Test
    void rdd() {
        List<Tuple2<String, double[]>> r = Q3Rdd.compute(censusFixture(), incomeFixture()).collect();
        assertEquals(2, r.size());
        assertEquals("90001", r.get(0)._1());
        assertEquals(24000.0, r.get(0)._2()[2], 1e-6);
        assertEquals("90002", r.get(1)._1());
        assertEquals(20000.0, r.get(1)._2()[2], 1e-6);
    }
}
