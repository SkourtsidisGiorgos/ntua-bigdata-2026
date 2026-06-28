package gr.ntua.dsml.q2;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.col;

/**
 * Query 2 — Spark SQL API. Same result as {@link Q2Df}, expressed as a SQL
 * window query. Both go through Catalyst, so the plans (and times) should be
 * close; that is the point of the comparison.
 */
public class Q2Sql {

    public static Dataset<Row> compute(SparkSession spark, Dataset<Row> crime) {
        crime.filter(col("year").isNotNull())
                .select("year", "month")
                .createOrReplaceTempView("crime");

        return spark.sql(
                "SELECT year, month, crime_total, ranking FROM (" +
                "  SELECT year, month, COUNT(*) AS crime_total, " +
                "         ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC, month ASC) AS ranking " +
                "  FROM crime GROUP BY year, month) t " +
                "WHERE ranking <= 3 " +
                "ORDER BY year ASC, crime_total DESC");
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q2-sql");

        String src;
        Dataset<Row> crime;
        if (args.length > 0) {
            crime = spark.read().parquet(args[0]);
            src = "parquet";
        } else {
            crime = Datasets.crime(spark);
            src = "csv";
        }

        Dataset<Row> ranked = compute(spark, crime);
        AtomicReference<List<Row>> box = new AtomicReference<>();
        Bench.time("q2-sql-" + src, () -> box.set(ranked.collectAsList()));
        Output.show("Q2 SQL (" + src + ")", box.get());

        spark.stop();
    }
}
