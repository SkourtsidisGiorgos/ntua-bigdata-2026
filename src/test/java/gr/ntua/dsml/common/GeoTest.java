package gr.ntua.dsml.common;

import gr.ntua.dsml.TestSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GeoTest {

    @Test
    void haversineKnownDistances() {
        // 1 degree of longitude at the equator ≈ 111.19 km.
        assertEquals(111.19, Geo.haversineKm(0, 0, 0, 1), 0.5);
        // Same point is zero distance.
        assertEquals(0.0, Geo.haversineKm(34.05, -118.25, 34.05, -118.25), 1e-9);
    }

    @Test
    void columnExpressionMatchesScalar() {
        double lat1 = 34.05, lon1 = -118.25, lat2 = 33.94, lon2 = -118.40;
        Dataset<Row> df = TestSpark.df(
                List.of(TestSpark.row(lat1, lon1, lat2, lon2)),
                TestSpark.dbl("lat1"), TestSpark.dbl("lon1"),
                TestSpark.dbl("lat2"), TestSpark.dbl("lon2"));

        double fromCol = df.select(
                        Geo.haversineKmCol(col("lat1"), col("lon1"), col("lat2"), col("lon2")))
                .first().getDouble(0);

        assertEquals(Geo.haversineKm(lat1, lon1, lat2, lon2), fromCol, 1e-6);
        assertTrue(fromCol > 0);
    }
}
