package gr.ntua.dsml.q1;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.TimeBucket;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.col;

/**
 * Query 1 — RDD API. The CSV is still read through Spark's DataFrameReader (so
 * the univocity parser handles quoting/types), then dropped to a JavaRDD&lt;Row&gt;
 * and processed with map/reduceByKey. No Catalyst optimization or columnar
 * scan applies here — that is the contrast with {@link Q1Df}.
 */
public class Q1Rdd {

    /** Input rows are (premis, time_occ). Output: part -> [street_count, total_count]. */
    public static JavaPairRDD<String, long[]> compute(JavaRDD<Row> rows) {
        return rows
                .filter(r -> !r.isNullAt(1))
                .mapToPair(r -> {
                    String part = TimeBucket.of(r.getInt(1));
                    long street = "STREET".equals(r.getString(0)) ? 1L : 0L;
                    return new Tuple2<>(part, new long[]{street, 1L});
                })
                .reduceByKey((a, b) -> new long[]{a[0] + b[0], a[1] + b[1]});
    }

    public static void printParts(String title, List<Tuple2<String, long[]>> agg) {
        long total = 0;
        for (Tuple2<String, long[]> t : agg) total += t._2()[1];

        record Part(String name, long street, double pct) {}
        List<Part> parts = new ArrayList<>();
        for (Tuple2<String, long[]> t : agg) {
            long street = t._2()[0];
            parts.add(new Part(t._1(), street, total == 0 ? 0.0 : 100.0 * street / total));
        }
        parts.sort((a, b) -> Double.compare(b.pct(), a.pct()));

        System.out.println();
        System.out.println("==== " + title + " — STREET share of all records, by part of day ====");
        System.out.println("part | street_crimes | total_records | pct(%)");
        for (Part p : parts) {
            System.out.printf("%-10s | %d | %d | %.4f%n", p.name(), p.street(), total, p.pct());
        }
        System.out.println();
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q1-rdd");
        JavaRDD<Row> rows = Datasets.crime(spark).select(col("premis"), col("time_occ")).javaRDD();
        JavaPairRDD<String, long[]> agg = compute(rows);

        AtomicReference<List<Tuple2<String, long[]>>> box = new AtomicReference<>();
        Bench.time("q1-rdd", () -> box.set(agg.collect()));
        printParts("Q1 RDD", box.get());

        spark.stop();
    }
}
