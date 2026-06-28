package gr.ntua.dsml.fmt;

import gr.ntua.dsml.common.Bench;
import gr.ntua.dsml.common.Datasets;
import gr.ntua.dsml.common.Paths;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Convert the crime CSV to a columnar format (Parquet) on HDFS.
 *
 * Writes the enriched crime DataFrame (with the derived date/time/lat/lon
 * columns) so the downstream queries can read it back unchanged. Compare the
 * query time on this output against CSV by running e.g.:
 *   submit.sh q2.Q2Df                        # reads CSV
 *   submit.sh q2.Q2Df &lt;parquet-path&gt;         # reads this Parquet
 *
 * Output path defaults to $USER/parquet/crime, override with args[0].
 */
public class Convert {

    public static void main(String[] args) {
        SparkSession spark = Datasets.session("fmt-convert");
        String out = args.length > 0 ? args[0] : Paths.USER + "/parquet/crime";

        Dataset<Row> crime = Datasets.crime(spark);
        Bench.time("convert-csv-to-parquet", () ->
                crime.write().mode("overwrite").parquet(out));

        System.out.println("Wrote Parquet -> " + out);
        spark.stop();
    }
}
