package gr.ntua.dsml.q4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static gr.ntua.dsml.TestSpark.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Q4: each crime is assigned to its nearest station; result is per-station
 * count + avg distance, ordered by count desc.
 *
 * Stations A(-118.20) and B(-118.00) at lat 34. Crimes at lon -118.19, -118.21
 * are nearest A; -118.01 is nearest B. Expected: A=2, B=1.
 */
class Q4Test {

    private static Dataset<Row> stationsFixture() {
        return df(List.of(
                        row("A", 34.00, -118.20),
                        row("B", 34.00, -118.00)),
                str("division"), dbl("lat"), dbl("lon"));
    }

    private static Dataset<Row> crimeFixture() {
        return df(List.of(
                        row(34.00, -118.19),
                        row(34.00, -118.21),
                        row(34.00, -118.01)),
                dbl("lat"), dbl("lon"));
    }

    @Test
    void nearestStationCountsAndOrder() {
        List<Row> r = Q4Df.compute(crimeFixture(), stationsFixture(), "broadcast").collectAsList();

        assertEquals(2, r.size());
        assertEquals("A", r.get(0).getString(r.get(0).fieldIndex("division")));
        assertEquals(2L, r.get(0).getLong(r.get(0).fieldIndex("#")));
        assertEquals("B", r.get(1).getString(r.get(1).fieldIndex("division")));
        assertEquals(1L, r.get(1).getLong(r.get(1).fieldIndex("#")));

        double avgA = r.get(0).getDouble(r.get(0).fieldIndex("average_distance"));
        assertTrue(avgA > 0 && avgA < 5, "avg distance should be a small positive km value");
    }
}
