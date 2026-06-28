package gr.ntua.dsml.common;

/**
 * Tiny timing helper so every query reports execution time the same way. The
 * printed line (prefixed BENCH) is easy to grep out of the driver log when
 * collecting numbers for the report's timing comparisons.
 */
public final class Bench {
    private Bench() {}

    public interface Action { void run() throws Exception; }

    /** Run {@code body}, print "BENCH <label>: <ms> ms", return elapsed ms. */
    public static long time(String label, Action body) {
        long t0 = System.nanoTime();
        try {
            body.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        System.out.println("BENCH " + label + ": " + ms + " ms");
        return ms;
    }
}
