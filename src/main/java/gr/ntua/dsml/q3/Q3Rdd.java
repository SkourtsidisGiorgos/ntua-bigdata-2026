package gr.ntua.dsml.q3;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.spark.sql.functions.col;

/**
 * Query 3 - RDD API. Same computation as Q3Df: aggregate census
 * population+households per ZIP, join with income, derive per-capita income.
 * The join here is an explicit JavaPairRDD.join (hash join) - no
 * Catalyst, so no broadcast/sort-merge choice is available, which is the
 * contrast the requirement asks us to comment on.
 */
public class Q3Rdd {

    /** Output: (zip, [population, median_income, per_capita_income]), sorted by per-capita desc. */
    public static JavaRDD<Tuple2<String, double[]>> compute(Dataset<Row> censusBlocks, Dataset<Row> income) {
        JavaPairRDD<String, long[]> byZip = censusBlocks
                .select(col("zip"), col("pop"), col("households")).javaRDD()
                .filter(r -> !r.isNullAt(0))
                .mapToPair(r -> new Tuple2<>(r.getString(0), new long[]{
                        r.isNullAt(1) ? 0L : r.getInt(1),
                        r.isNullAt(2) ? 0L : r.getInt(2)}))
                .reduceByKey((a, b) -> new long[]{a[0] + b[0], a[1] + b[1]})
                .filter(t -> t._2()[0] > 0);

        JavaPairRDD<String, Integer> inc = income
                .filter(col("income").isNotNull())
                .select(col("zip"), col("income")).javaRDD()
                .filter(r -> !r.isNullAt(0))
                .mapToPair(r -> new Tuple2<>(r.getString(0), r.getInt(1)));

        return byZip.join(inc).map(t -> {
            long pop = t._2()._1()[0];
            long households = t._2()._1()[1];
            int medianIncome = t._2()._2();
            double perCapita = (double) medianIncome * households / pop;
            return new Tuple2<>(t._1(), new double[]{pop, medianIncome, perCapita});
        }).sortBy(t -> t._2()[2], false, 1);
    }

    public static void printResult(String title, List<Tuple2<String, double[]>> rows) {
        System.out.println();
        System.out.println("==== " + title + " (" + rows.size() + " rows) ====");
        System.out.println("zip | pop | income | per_capita_income");
        for (Tuple2<String, double[]> t : rows) {
            double[] v = t._2();
            System.out.printf("%s | %d | %d | %.2f%n", t._1(), (long) v[0], (long) v[1], v[2]);
        }
        System.out.println();
    }

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("q3-rdd");
        JavaRDD<Tuple2<String, double[]>> result =
                compute(Datasets.censusPopulation(spark), Datasets.income(spark));

        AtomicReference<List<Tuple2<String, double[]>>> box = new AtomicReference<>();
        Bench.time("q3-rdd", () -> box.set(result.collect()));
        printResult("Q3 RDD (per-capita income by ZIP)", box.get());

        spark.stop();
    }
}
