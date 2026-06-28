package gr.ntua.dsml.q1;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.TimeBucket;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.*;

/**
 * Query 1 - pure DataFrame API.
 *
 * Ranks the parts of the day by the share of crime records that took place on
 * the STREET. The denominator is the total number of records (constant across parts),
 * so the ranking is the same whichever total is used; we report thepercentage against ALL records.
 *
 * The time-of-day bucketing is a Catalyst `CASE WHEN` (see TimeBucket#column);
 * Q1DfUdf does the same via a UDF and Q1Rdd via the RDD API, so the three are directly comparable.
 */
public class Q1Df {

    /** One row per part of day: part, street count, total count. */
    public static Dataset<Row> compute(Dataset<Row> crime) {
        return crime
                .filter(col("time_occ").isNotNull())
                .withColumn("part", TimeBucket.column(col("time_occ")))
                .groupBy(col("part"))
                .agg(
                        sum(when(col("premis").equalTo("STREET"), 1).otherwise(0)).alias("street"),
                        count(lit(1)).alias("total"));
    }

    /** Shared by Q1Df and Q1DfUdf: turn the (part, street, total) rows into the ranked report. */
    public static void printParts(String title, List<Row> agg) {
        long total = 0;
        for (Row r : agg) total += r.getLong(r.fieldIndex("total"));

        record Part(String name, long street, double pct) {}
        List<Part> parts = new ArrayList<>();
        for (Row r : agg) {
            long street = r.getLong(r.fieldIndex("street"));
            parts.add(new Part(r.getString(r.fieldIndex("part")), street,
                    total == 0 ? 0.0 : 100.0 * street / total));
        }
        parts.sort((a, b) -> Double.compare(b.pct(), a.pct()));

        System.out.println();
        System.out.println("- " + title + " - STREET share of all records, by part of day -");
        System.out.println("part | street_crimes | total_records | pct(%)");
        for (Part p : parts) {
            System.out.printf("%-10s | %d | %d | %.4f%n", p.name(), p.street(), total, p.pct());
        }
        System.out.println();
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q1-df");
        Dataset<Row> agg = compute(Datasets.crime(spark));

        AtomicReference<List<Row>> box = new AtomicReference<>();
        Bench.time("q1-df", () -> box.set(agg.collectAsList()));
        printParts("Q1 DataFrame", box.get());

        spark.stop();
    }
}
