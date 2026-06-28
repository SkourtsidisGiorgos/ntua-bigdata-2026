package gr.ntua.dsml.q1;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.TimeBucket;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.*;

/**
 * Query 1 DataFrame API but with the time-of-day classification done by a user-defined function instead of a native {@code CASE WHEN}.
 * Same result as {@link Q1Df};
 * the point is to compare cost (a UDF is a black box to Catalyst, so it cannot be optimized/codegen-fused and pays serialization overhead).
 */
public class Q1DfUdf {

    public static final String UDF_NAME = "part_of_day";

    /** Registers the bucketing UDF and runs the same aggregation as Q1Df#compute. */
    public static Dataset<Row> compute(SparkSession spark, Dataset<Row> crime) {
        spark.udf().register(UDF_NAME,
                (UDF1<Integer, String>) t -> t == null ? null : TimeBucket.of(t),
                DataTypes.StringType);

        return crime
                .filter(col("time_occ").isNotNull())
                .withColumn("part", callUDF(UDF_NAME, col("time_occ")))
                .groupBy(col("part"))
                .agg(
                        sum(when(col("premis").equalTo("STREET"), 1).otherwise(0)).alias("street"),
                        count(lit(1)).alias("total"));
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q1-df-udf");
        Dataset<Row> agg = compute(spark, Datasets.crime(spark));

        AtomicReference<List<Row>> box = new AtomicReference<>();
        Bench.time("q1-df-udf", () -> box.set(agg.collectAsList()));
        Q1Df.printParts("Q1 DataFrame+UDF", box.get());

        spark.stop();
    }
}
