package gr.ntua.dsml.common;

import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

/**
 * Time-of-day segmentation used by Query 1, expressed once so the DataFrame,
 * UDF and RDD implementations all agree.
 *
 * TIME OCC is an HHMM integer (e.g. 1350 -> 13:50, 845 -> 08:45). Segments:
 *   Morning   : 05:00 - 11:59   (0500-1159)
 *   Afternoon : 12:00 - 16:59   (1200-1659)
 *   Evening   : 17:00 - 20:59   (1700-2059)
 *   Night     : 21:00 - 04:59   (2100-2359 and 0000-0459)
 */
public final class TimeBucket {
    private TimeBucket() {}

    public static final String MORNING   = "Morning";
    public static final String AFTERNOON = "Afternoon";
    public static final String EVENING   = "Evening";
    public static final String NIGHT     = "Night";

    /** Plain-Java classification, for RDD and UDF use. {@code hhmm} is HOUR*100+MINUTE. */
    public static String of(int hhmm) {
        if (hhmm >= 500  && hhmm <= 1159) return MORNING;
        if (hhmm >= 1200 && hhmm <= 1659) return AFTERNOON;
        if (hhmm >= 1700 && hhmm <= 2059) return EVENING;
        return NIGHT; // 2100-2359 and 0-459
    }

    /** Column expression over an integer HHMM column, for the pure-DataFrame impl. */
    public static Column column(Column hhmm) {
        return when(hhmm.between(500, 1159), lit(MORNING))
                .when(hhmm.between(1200, 1659), lit(AFTERNOON))
                .when(hhmm.between(1700, 2059), lit(EVENING))
                .otherwise(lit(NIGHT));
    }
}
