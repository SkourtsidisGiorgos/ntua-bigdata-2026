package gr.ntua.dsml.common;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Applies a join-strategy hint to one side of a join (Requirement 6). The hint
 * tells Catalyst which physical join to prefer; {@code explain()} then shows
 * what it actually picked.
 *
 * Spark's join hints: BROADCAST, MERGE (sort-merge), SHUFFLE_HASH,
 * SHUFFLE_REPLICATE_NL (shuffle-and-replicate nested loop). The last one is the
 * only non-equi strategy and is what a cross join can fall back to.
 */
public final class JoinStrategy {
    private JoinStrategy() {}

    /** Names accepted on the command line, in the order the report iterates them. */
    public static final String[] ALL =
            {"broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"};

    /** Return {@code df} with the requested hint, or unchanged for null/"default". */
    public static Dataset<Row> hint(Dataset<Row> df, String strategy) {
        if (strategy == null) return df;
        switch (strategy.toLowerCase()) {
            case "":
            case "default":
            case "none":
                return df;
            default:
                return df.hint(strategy);
        }
    }
}
