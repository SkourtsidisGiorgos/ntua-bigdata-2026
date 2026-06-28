package gr.ntua.dsml.common;

import gr.ntua.dsml.TestSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for the custom GeoJSON loader ({@link Datasets#censusPopulation})
 * against a tiny in-repo fixture, so the line-by-line parse (keep Feature lines,
 * drop the FeatureCollection wrapper, strip the trailing comma, JSON-parse each
 * Feature, project ZCTA20/POP20/HOUSING20) is exercised on every CI run — not
 * only when the 180MB real file happens to be present (see LoaderLocalTest).
 *
 * Fixture: 3 Feature lines (last one without a trailing comma), zips
 * 90001 x2 + 90002. The loader returns block-level rows (no aggregation).
 */
class CensusLoaderTest {

    private static final String FIXTURE = "src/test/resources/census_tiny.geojson";

    @Test
    void parsesFeaturesDropsWrapperStripsTrailingComma() {
        Dataset<Row> df = Datasets.censusPopulation(TestSpark.get(), FIXTURE);

        // One row per Feature; the "{", "type", "name", "features": [, "]", "}"
        // wrapper lines must not leak in.
        assertEquals(3, df.count());
        assertEquals(2, df.select("zip").distinct().count());

        // Properties parsed with the right types/values (pop+households are int).
        Row agg = df.groupBy()
                .agg(sum(col("pop")).alias("pop"), sum(col("households")).alias("hh"))
                .first();
        assertEquals(350L, agg.getLong(agg.fieldIndex("pop")));   // 100+50+200
        assertEquals(110L, agg.getLong(agg.fieldIndex("hh")));    // 40+20+50
    }
}
