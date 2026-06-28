package gr.ntua.dsml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * One local SparkSession shared by all tests (built lazily, never stopped — the
 * JVM teardown reclaims it). Keeping a single SparkContext per JVM avoids the
 * cost and fragility of stopping/restarting it between test classes.
 */
public final class TestSpark {
    private static SparkSession instance;

    public static synchronized SparkSession get() {
        if (instance == null) {
            instance = SparkSession.builder()
                    .appName("dsml-test")
                    .master("local[2]")
                    .config("spark.ui.enabled", "false")
                    .config("spark.sql.shuffle.partitions", "2")
                    .getOrCreate();
            instance.sparkContext().setLogLevel("WARN");
        }
        return instance;
    }

    /** Build a small DataFrame from rows; each field is (name, DataType). */
    public static Dataset<Row> df(List<Row> rows, StructField... fields) {
        return get().createDataFrame(rows, DataTypes.createStructType(fields));
    }

    public static StructField str(String name) {
        return DataTypes.createStructField(name, DataTypes.StringType, true);
    }

    public static StructField intf(String name) {
        return DataTypes.createStructField(name, DataTypes.IntegerType, true);
    }

    public static StructField dbl(String name) {
        return DataTypes.createStructField(name, DataTypes.DoubleType, true);
    }

    public static Row row(Object... values) {
        return RowFactory.create(values);
    }

    private TestSpark() {}
}
