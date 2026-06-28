package gr.ntua.dsml.common;

import org.apache.spark.sql.Row;

import java.util.List;

/**
 * Prints an already-collected result to stdout (the driver pod log, which
 * scripts/logs.sh fetches). We print from a collected List rather than calling
 * Dataset.show() so the Spark job is triggered exactly once - inside Bench.time
 * - and the measured time is not inflated by a second pass for display.
 */
public final class Output {
    private Output() {}

    public static void show(String title, List<Row> rows) {
        System.out.println();
        System.out.println("==== " + title + " (" + rows.size() + " rows) ====");
        if (!rows.isEmpty() && rows.get(0).schema() != null) {
            System.out.println(String.join(" | ", rows.get(0).schema().fieldNames()));
        }
        for (Row r : rows) {
            System.out.println(r.mkString(" | "));
        }
        System.out.println();
    }
}
