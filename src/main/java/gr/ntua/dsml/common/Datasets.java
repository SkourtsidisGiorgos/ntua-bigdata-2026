package gr.ntua.dsml.common;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.Encoders.STRING;
import static org.apache.spark.sql.functions.*;

/**
 * Shared DataFrame loaders. Each returns a tidy, typed frame with normalized
 * column names so the per-query code stays focused on the actual logic.
 */
public final class Datasets {
    private Datasets() {}

    /** A SparkSession with the given app name (cluster config comes from spark-submit). */
    public static SparkSession session(String appName) {
        return SparkSession.builder().appName(appName).getOrCreate();
    }

    private static final String DATE_OCC_FMT = "yyyy MMM dd hh:mm:ss a"; // 2010 Feb 20 12:00:00 AM

    /**
     * Both crime CSVs unioned, with derived columns:
     *   date_occ (date), year, month (int), time_occ (int HHMM),
     *   premis (string), area (string), lat, lon (double).
     * Original columns are preserved too.
     *
     * @param path optional override (single file). When null, loads + unions both files.
     */
    public static Dataset<Row> crime(SparkSession spark, String path) {
        String[] paths = (path != null)
                ? new String[]{path}
                : new String[]{Paths.CRIME_2010_2019, Paths.CRIME_2020_2025};

        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("quote", "\"")
                .option("escape", "\"")
                .option("multiLine", "false")
                .csv(paths);

        return raw
                .withColumn("date_occ", to_date(col("DATE OCC"), DATE_OCC_FMT))
                .withColumn("year", year(col("date_occ")))
                .withColumn("month", month(col("date_occ")))
                .withColumn("time_occ", col("TIME OCC").cast("int"))
                .withColumn("premis", col("Premis Desc"))
                .withColumn("area", col("AREA NAME"))
                .withColumn("lat", col("LAT").cast("double"))
                .withColumn("lon", col("LON").cast("double"));
    }

    public static Dataset<Row> crime(SparkSession spark) {
        return crime(spark, null);
    }

    /**
     * Median household income per ZIP (2021). The file is ';'-delimited and the
     * income is '$'-formatted ("$52,806"). Returns columns: zip (string),
     * income (int).
     */
    public static Dataset<Row> income(SparkSession spark) {
        return income(spark, Paths.INCOME_2021);
    }

    /** Same as {@link #income(SparkSession)} but from an explicit path (used by local tests). */
    public static Dataset<Row> income(SparkSession spark, String path) {
        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .csv(path);

        return raw
                .withColumn("zip", col("Zip Code").cast("string"))
                .withColumn("income",
                        regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("int"))
                .select("zip", "income");
    }

    /**
     * Police stations. Header carries a UTF-8 BOM, so columns are renamed by
     * position. Returns: division (string), lat (double), lon (double), prec.
     * NOTE in this file X = longitude, Y = latitude.
     */
    public static Dataset<Row> policeStations(SparkSession spark) {
        return policeStations(spark, Paths.POLICE_STATIONS);
    }

    /** Same as {@link #policeStations(SparkSession)} but from an explicit path (local tests). */
    public static Dataset<Row> policeStations(SparkSession spark, String path) {
        Dataset<Row> raw = spark.read()
                .option("header", "true")
                .csv(path)
                .toDF("x", "y", "fid", "division", "location", "prec");

        return raw
                .withColumn("lon", col("x").cast("double"))
                .withColumn("lat", col("y").cast("double"))
                .select(col("division"), col("lat"), col("lon"),
                        col("prec").cast("int").alias("prec"));
    }

    /**
     * Census-block population by ZIP. Returns block-level rows: zip = ZCTA20
     * (string), pop = POP20 (int), households = HOUSING20 (int). Aggregate
     * (sum by zip) downstream.
     *
     * The GeoJSON is a FeatureCollection physically laid out one Feature per
     * line. Reading it with multiLine=true forces the whole 180MB file into a
     * single record/task and OOMs small executors, so instead we read it as
     * text, keep the per-Feature lines (dropping the wrapper), strip the
     * trailing comma, and JSON-parse line by line - fully distributed and
     * memory-light. Only the needed properties are kept; geometry is discarded.
     */
    public static Dataset<Row> censusPopulation(SparkSession spark) {
        return censusPopulation(spark, Paths.CENSUS_BLOCKS);
    }

    /** Same as {@link #censusPopulation(SparkSession)} but from an explicit path (local tests). */
    public static Dataset<Row> censusPopulation(SparkSession spark, String path) {
        Dataset<String> featureLines = spark.read()
                .textFile(path)
                .filter((FilterFunction<String>)
                        s -> s.trim().startsWith("{ \"type\": \"Feature\""))
                .map((MapFunction<String, String>) s -> {
                    String t = s.trim();
                    return t.endsWith(",") ? t.substring(0, t.length() - 1) : t;
                }, STRING());

        return spark.read().json(featureLines)
                .select(
                        col("properties.ZCTA20").cast("string").alias("zip"),
                        col("properties.POP20").cast("int").alias("pop"),
                        col("properties.HOUSING20").cast("int").alias("households"));
    }
}
