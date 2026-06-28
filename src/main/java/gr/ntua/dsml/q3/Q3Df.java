package gr.ntua.dsml.q3;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.JoinStrategy;
import gr.ntua.dsml.common.Output;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.*;

/**
 * Query 3 - DataFrame API. Average annual per-capita income per ZIP for
 * 2020–2021, joining 2020 census population with 2021 median household income.
 *
 * The income dataset is per-household, so we convert it to per-capita using the
 * average household size derived from the census:
 *   per_capita = median_household_income / (population / households)
 *             = median_household_income * households / population
 * (households approximated by HOUSING20 housing units.)
 *
 * main() takes an optional join-strategy hint as args[0] (Requirement 6) and
 * prints explain() so the chosen physical join is visible in the log.
 */
public class Q3Df {

    public static Dataset<Row> compute(Dataset<Row> censusBlocks, Dataset<Row> income, String strategy) {
        Dataset<Row> byZip = censusBlocks
                .groupBy(col("zip"))
                .agg(sum(col("pop")).alias("pop"),
                     sum(col("households")).alias("households"))
                .filter(col("pop").gt(0));

        Dataset<Row> inc = income.filter(col("income").isNotNull());

        return byZip.join(JoinStrategy.hint(inc, strategy), "zip")
                .withColumn("per_capita_income",
                        round(col("income").multiply(col("households")).divide(col("pop")), 2))
                .select(col("zip"), col("pop"), col("income"), col("per_capita_income"))
                .orderBy(col("per_capita_income").desc());
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q3-df");
        String strategy = args.length > 0 ? args[0] : "default";

        Dataset<Row> joined = compute(
                Datasets.censusPopulation(spark), Datasets.income(spark), strategy);

        System.out.println("== Q3 DataFrame - join strategy hint: " + strategy + " ==");
        joined.explain();

        AtomicReference<List<Row>> box = new AtomicReference<>();
        Bench.time("q3-df-" + strategy, () -> box.set(joined.collectAsList()));
        Output.show("Q3 DataFrame (per-capita income by ZIP)", box.get());

        spark.stop();
    }
}
