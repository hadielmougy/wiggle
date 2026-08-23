package dev.wiggle.tests;

import dev.wiggle.core.Cron;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The pure cron evaluator: five-field parsing and next-fire computation, all in UTC. */
class CronTest {

    private static long at(String isoUtc) {
        return Instant.parse(isoUtc).toEpochMilli();
    }

    private static String next(String cron, String afterIsoUtc) {
        return Instant.ofEpochMilli(Cron.parse(cron).next(at(afterIsoUtc))).toString();
    }

    @Test @DisplayName("fixed time of day fires today if still ahead, else tomorrow")
    void fixedTime() {
        assertEquals("2026-08-23T14:30:00Z", next("30 14 * * *", "2026-08-23T10:00:00Z"));
        assertEquals("2026-08-24T14:30:00Z", next("30 14 * * *", "2026-08-23T15:00:00Z"));
        assertEquals("2026-08-24T14:30:00Z", next("30 14 * * *", "2026-08-23T14:30:00Z"),
                "strictly after: firing at 14:30 re-arms to tomorrow");
    }

    @Test @DisplayName("steps, ranges and lists")
    void stepsRangesLists() {
        assertEquals("2026-08-23T10:15:00Z", next("*/15 * * * *", "2026-08-23T10:07:00Z"));
        assertEquals("2026-08-23T11:00:00Z", next("*/15 * * * *", "2026-08-23T10:45:00Z"));
        assertEquals("2026-08-23T09:00:00Z", next("0 9-17 * * *", "2026-08-23T08:10:00Z"));
        assertEquals("2026-08-24T09:00:00Z", next("0 9-17 * * *", "2026-08-23T17:30:00Z"),
                "past the range: next day");
        assertEquals("2026-08-23T10:40:00Z", next("10,40 * * * *", "2026-08-23T10:15:00Z"));
        assertEquals("2026-08-23T10:30:00Z", next("10-50/20 * * * *", "2026-08-23T10:12:00Z"));
    }

    @Test @DisplayName("day-of-month, month and day-of-week fields")
    void dateFields() {
        assertEquals("2026-09-01T00:00:00Z", next("0 0 1 * *", "2026-08-23T10:00:00Z"), "first of month");
        assertEquals("2026-08-24T09:00:00Z", next("0 9 * * 1", "2026-08-23T10:00:00Z"),
                "2026-08-23 is a Sunday; next Monday is the 24th");
        assertEquals("2026-08-30T09:00:00Z", next("0 9 * * 0", "2026-08-23T10:00:00Z"), "0 = Sunday");
        assertEquals("2026-08-30T09:00:00Z", next("0 9 * * 7", "2026-08-23T10:00:00Z"), "7 = Sunday too");
        assertEquals("2026-12-25T08:00:00Z", next("0 8 25 12 *", "2026-08-23T10:00:00Z"), "yearly");
    }

    @Test @DisplayName("vixie rule: restricted day-of-month OR day-of-week")
    void domDowOr() {
        // "13th or Friday": from Sun 2026-08-23 the next Friday (28th) comes before the next 13th.
        assertEquals("2026-08-28T00:00:00Z", next("0 0 13 * 5", "2026-08-23T10:00:00Z"));
        // With dom '*' the dow alone decides; with dow '*' the dom alone decides.
        assertEquals("2026-09-13T00:00:00Z", next("0 0 13 * *", "2026-08-23T10:00:00Z"));
    }

    @Test @DisplayName("invalid expressions are rejected; an impossible date never fires")
    void validation() {
        assertThrows(IllegalArgumentException.class, () -> Cron.parse("* * * *"), "four fields");
        assertThrows(IllegalArgumentException.class, () -> Cron.parse("60 * * * *"), "minute out of range");
        assertThrows(IllegalArgumentException.class, () -> Cron.parse("* 24 * * *"), "hour out of range");
        assertThrows(IllegalArgumentException.class, () -> Cron.parse("* * 0 * *"), "dom out of range");
        assertThrows(IllegalArgumentException.class, () -> Cron.parse("a * * * *"), "not a number");
        assertThrows(IllegalArgumentException.class, () -> Cron.parse("*/0 * * * *"), "zero step");
        assertThrows(IllegalStateException.class,
                () -> Cron.parse("0 0 30 2 *").next(at("2026-08-23T10:00:00Z")), "Feb 30 never comes");
    }
}
