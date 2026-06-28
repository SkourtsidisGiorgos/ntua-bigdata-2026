package gr.ntua.dsml;

import gr.ntua.dsml.common.Datasets;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

/** Validates every common loader against the real HDFS data. Read-only. */
public class ProbeCommon {
    public static void main(String[] args) {
        SparkSession spark = Datasets.session("probe-common");

        System.out.println("==== income ====");
        Dataset<Row> income = Datasets.income(spark);
        income.show(5, false);
        System.out.println("income rows = " + income.count()
                + ", null income = " + income.filter(col("income").isNull()).count());

        System.out.println("==== police stations ====");
        Dataset<Row> stations = Datasets.policeStations(spark);
        stations.show(5, false);
        System.out.println("station rows = " + stations.count());

        System.out.println("==== census population ====");
        Dataset<Row> census = Datasets.censusPopulation(spark);
        census.show(5, false);
        System.out.println("census block rows = " + census.count()
                + ", distinct zips = " + census.select("zip").distinct().count());

        System.out.println("==== crime (sample, parse check) ====");
        Dataset<Row> crime = Datasets.crime(spark);
        crime.select("DATE OCC", "date_occ", "year", "month", "TIME OCC", "time_occ", "premis")
                .show(5, false);
        // Cheap parse-health check on a sample instead of a full 940MB count.
        Dataset<Row> sample = crime.select("year", "time_occ").limit(100000).cache();
        System.out.println("sample=" + sample.count()
                + ", null year=" + sample.filter(col("year").isNull()).count()
                + ", null time=" + sample.filter(col("time_occ").isNull()).count());

        spark.stop();
    }
}
