package gr.ntua.dsml.common;

import gr.ntua.dsml.TestSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Loader checks against the real datasets in ./data (downloaded from HDFS for
 * local EDA). Each test is skipped if its file is absent, so the suite still
 * passes on a machine without the data. Run after pulling data with the EDA
 * download step.
 */
class LoaderLocalTest {

    private static final String DATA = "data";

    private static boolean has(String rel) {
        return new File(DATA + "/" + rel).exists();
    }

    @Test
    void incomeHas282RowsAnd2NullIncomes() {
        assumeTrue(has("LA_income_2021.csv"));
        Dataset<Row> df = Datasets.income(TestSpark.get(), DATA + "/LA_income_2021.csv");
        assertEquals(282, df.count());
        assertEquals(2, df.filter(col("income").isNull()).count());
    }

    @Test
    void policeStationsHas21Rows() {
        assumeTrue(has("LA_Police_Stations.csv"));
        Dataset<Row> df = Datasets.policeStations(TestSpark.get(), DATA + "/LA_Police_Stations.csv");
        assertEquals(21, df.count());
        // BOM handling: division must be a real name, not an empty/garbled token.
        assertEquals(0, df.filter(col("division").isNull()).count());
    }

    @Test
    void censusZipsInExpectedRange() {
        assumeTrue(has("LA_Census_Blocks_2020.geojson"));
        Dataset<Row> df = Datasets.censusPopulation(TestSpark.get(), DATA + "/LA_Census_Blocks_2020.geojson");
        long zips = df.select("zip").distinct().count();
        assertTrue(zips > 250 && zips < 400, "distinct zips was " + zips);
    }

    @Test
    void crimeDateAndTimeParseCleanlyOnSample() {
        assumeTrue(has("LA_Crime_Data/LA_Crime_Data_2010_2019.csv"));
        Dataset<Row> df = Datasets.crime(TestSpark.get(), DATA + "/LA_Crime_Data/LA_Crime_Data_2010_2019.csv");
        Dataset<Row> sample = df.select("year", "time_occ").limit(2000).cache();
        long total = sample.count();
        // The vast majority of rows should parse to a non-null year and time.
        assertTrue(sample.filter(col("year").isNotNull()).count() > total * 0.95);
        assertTrue(sample.filter(col("time_occ").isNotNull()).count() > total * 0.95);
    }
}
