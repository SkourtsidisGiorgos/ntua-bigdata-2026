package gr.ntua.dsml.q2;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.*;

/**
 * Query 2 DataFrame API. For each year, the 3 months with the most crimes,
 * with their rank. Output ordered by year asc, crime_total desc.
 * Ties broken by month asc so the top-3 is deterministic (row_number rather than rank, which could emit more than 3 rows on a tie).
 * main() reads from CSV by default, or from a Parquet path given as args[0]
 */
public class Q2Df {

    public static Dataset<Row> compute(Dataset<Row> crime) {
        Dataset<Row> counts = crime
                .filter(col("year").isNotNull())
                .groupBy(col("year"), col("month"))
                .count();

        WindowSpec byYear = Window.partitionBy(col("year"))
                .orderBy(col("count").desc(), col("month").asc());

        return counts
                .withColumn("ranking", row_number().over(byYear))
                .filter(col("ranking").leq(3))
                .select(col("year"), col("month"),
                        col("count").alias("crime_total"), col("ranking"))
                .orderBy(col("year").asc(), col("crime_total").desc());
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q2-df");

        String src;
        Dataset<Row> crime;
        if (args.length > 0) {
            crime = spark.read().parquet(args[0]);
            src = "parquet";
        } else {
            crime = Datasets.crime(spark);
            src = "csv";
        }

        Dataset<Row> ranked = compute(crime);
        AtomicReference<List<Row>> box = new AtomicReference<>();
        Bench.time("q2-df-" + src, () -> box.set(ranked.collectAsList()));
        Output.show("Q2 DataFrame (" + src + ")", box.get());

        spark.stop();
    }
}
