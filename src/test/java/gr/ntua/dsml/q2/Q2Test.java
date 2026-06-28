package gr.ntua.dsml.q2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static gr.ntua.dsml.TestSpark.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Q2: DataFrame and SQL must return the same top-3-months-per-year ranking.
 * Expected rows (year, month, crime_total, ranking), ordered year asc then
 * crime_total desc, with month-asc tie-break:
 *   2010: (2,3,1) (3,2,2) (1,1,3)   -- month 1 beats month 5 (both count 1)
 *   2011: (12,4,1) (6,2,2) (4,1,3)
 */
class Q2Test {

    private static final long[][] EXPECTED = {
            {2010, 2, 3, 1}, {2010, 3, 2, 2}, {2010, 1, 1, 3},
            {2011, 12, 4, 1}, {2011, 6, 2, 2}, {2011, 4, 1, 3},
    };

    private static Dataset<Row> crimeFixture() {
        int[][] spec = {{2010, 2, 3}, {2010, 3, 2}, {2010, 5, 1}, {2010, 1, 1},
                        {2011, 12, 4}, {2011, 6, 2}, {2011, 4, 1}};
        List<Row> rows = new ArrayList<>();
        for (int[] s : spec) {
            for (int i = 0; i < s[2]; i++) rows.add(row(s[0], s[1]));
        }
        return df(rows, intf("year"), intf("month"));
    }

    private static void assertMatchesExpected(List<Row> rows) {
        assertEquals(EXPECTED.length, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            Row r = rows.get(i);
            long[] actual = {
                    r.getInt(r.fieldIndex("year")),
                    r.getInt(r.fieldIndex("month")),
                    r.getLong(r.fieldIndex("crime_total")),
                    r.getInt(r.fieldIndex("ranking"))};
            assertArrayEquals(EXPECTED[i], actual, "row " + i);
        }
    }

    @Test
    void dataFrame() {
        assertMatchesExpected(Q2Df.compute(crimeFixture()).collectAsList());
    }

    @Test
    void sql() {
        assertMatchesExpected(Q2Sql.compute(get(), crimeFixture()).collectAsList());
    }
}
