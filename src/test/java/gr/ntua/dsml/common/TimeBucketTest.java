package gr.ntua.dsml.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeBucketTest {

    @Test
    void boundariesMapToTheRightPartOfDay() {
        // Morning 05:00–11:59
        assertEquals(TimeBucket.MORNING, TimeBucket.of(500));
        assertEquals(TimeBucket.MORNING, TimeBucket.of(1159));
        // Afternoon 12:00–16:59
        assertEquals(TimeBucket.AFTERNOON, TimeBucket.of(1200));
        assertEquals(TimeBucket.AFTERNOON, TimeBucket.of(1659));
        // Evening 17:00–20:59
        assertEquals(TimeBucket.EVENING, TimeBucket.of(1700));
        assertEquals(TimeBucket.EVENING, TimeBucket.of(2059));
        // Night 21:00–04:59 (wraps midnight)
        assertEquals(TimeBucket.NIGHT, TimeBucket.of(2100));
        assertEquals(TimeBucket.NIGHT, TimeBucket.of(2359));
        assertEquals(TimeBucket.NIGHT, TimeBucket.of(0));
        assertEquals(TimeBucket.NIGHT, TimeBucket.of(459));
    }
}
