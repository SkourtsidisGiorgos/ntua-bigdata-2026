package gr.ntua.dsml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Harness smoke test: read a small dataset from HDFS, print schema + count.
 * Proves cluster submission, HDFS access, and the jar's classpath all work.
 *
 * Run via scripts/submit.sh (class = gr.ntua.dsml.Smoke).
 */
public class Smoke {

    private static final String HDFS = "hdfs://hdfs-namenode:9000";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("smoke")
                .getOrCreate();

        // Small dataset (21 police stations) keeps the smoke test fast.
        String path = (args.length > 0) ? args[0] : HDFS + "/data/LA_Police_Stations.csv";

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(path);

        System.out.println("==== SMOKE: " + path + " ====");
        df.printSchema();
        System.out.println("row count = " + df.count());
        df.show(5, false);

        spark.stop();
    }
}
