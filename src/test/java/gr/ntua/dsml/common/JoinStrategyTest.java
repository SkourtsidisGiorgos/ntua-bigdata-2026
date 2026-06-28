package gr.ntua.dsml.common;

import gr.ntua.dsml.TestSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link JoinStrategy#hint} branch logic: null/empty/"default"/"none" (any case)
 * are no-ops returning the same frame, while a real strategy name attaches a
 * join hint to the logical plan.
 */
class JoinStrategyTest {

    private static Dataset<Row> sample() {
        return TestSpark.df(List.of(TestSpark.row("90001", 1)),
                TestSpark.str("zip"), TestSpark.intf("x"));
    }

    @Test
    void noOpStrategiesReturnSameFrame() {
        Dataset<Row> df = sample();
        assertSame(df, JoinStrategy.hint(df, null));
        assertSame(df, JoinStrategy.hint(df, ""));
        assertSame(df, JoinStrategy.hint(df, "default"));
        assertSame(df, JoinStrategy.hint(df, "none"));
        assertSame(df, JoinStrategy.hint(df, "DEFAULT")); // case-insensitive
    }

    @Test
    void realStrategyAttachesHint() {
        Dataset<Row> df = sample();
        Dataset<Row> hinted = JoinStrategy.hint(df, "broadcast");

        assertNotSame(df, hinted);
        // The hint shows up as an UnresolvedHint node in the logical plan.
        String plan = hinted.queryExecution().logical().treeString().toLowerCase();
        assertTrue(plan.contains("hint") || plan.contains("broadcast"),
                "logical plan should carry the join hint:\n" + plan);
    }
}
