package com.wiggle.core;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;

/**
 * A standard five-field cron expression ({@code minute hour day-of-month month day-of-week}),
 * supporting {@code *}, values, lists ({@code 1,5}), ranges ({@code 1-5}) and steps
 * ({@code *}{@code /15}, {@code 10-40/10}). Day-of-week accepts 0-7 with both 0 and 7 as Sunday.
 * As in vixie cron, when day-of-month and day-of-week are both restricted a day matches if
 * <em>either</em> does. Fire times are evaluated in <strong>UTC</strong>, so every node in a
 * cluster computes the same schedule regardless of its local zone.
 */
public final class Cron {

    private final BitSet minutes;
    private final BitSet hours;
    private final BitSet daysOfMonth;
    private final BitSet months;
    private final BitSet daysOfWeek;
    private final boolean domRestricted;
    private final boolean dowRestricted;
    private final String expression;

    private Cron(String expression, BitSet minutes, BitSet hours, BitSet daysOfMonth, BitSet months,
                 BitSet daysOfWeek, boolean domRestricted, boolean dowRestricted) {
        this.expression = expression;
        this.minutes = minutes;
        this.hours = hours;
        this.daysOfMonth = daysOfMonth;
        this.months = months;
        this.daysOfWeek = daysOfWeek;
        this.domRestricted = domRestricted;
        this.dowRestricted = dowRestricted;
    }

    public static Cron parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("cron expression is required");
        }
        String[] fields = expression.trim().split("\\s+");
        if (fields.length != 5) {
            throw new IllegalArgumentException("cron '" + expression
                    + "' must have 5 fields (minute hour day-of-month month day-of-week)");
        }
        BitSet dow = field(fields[4], 0, 7);
        if (dow.get(7)) dow.set(0);   // 7 is Sunday too
        return new Cron(expression.trim(),
                field(fields[0], 0, 59), field(fields[1], 0, 23), field(fields[2], 1, 31),
                field(fields[3], 1, 12), dow,
                !fields[2].equals("*"), !fields[4].equals("*"));
    }

    /** Parses one field into the set of matching values within {@code [min, max]}. */
    private static BitSet field(String spec, int min, int max) {
        BitSet set = new BitSet(max + 1);
        for (String part : spec.split(",")) {
            addPart(set, part, min, max, spec);
        }
        return set;
    }

    private static void addPart(BitSet set, String part, int min, int max, String spec) {
        int step = 1;
        String range = part;
        int slash = part.indexOf('/');
        if (slash >= 0) {
            step = parseInt(part.substring(slash + 1), spec);
            range = part.substring(0, slash);
            if (step < 1) throw new IllegalArgumentException("cron field '" + spec + "': step must be >= 1");
        }
        int from;
        int to;
        if (range.equals("*")) {
            from = min;
            to = max;
        } else if (range.contains("-")) {
            String[] bounds = range.split("-", 2);
            from = parseInt(bounds[0], spec);
            to = parseInt(bounds[1], spec);
        } else {
            from = parseInt(range, spec);
            to = slash >= 0 ? max : from;   // "N/step" means "from N to the end, stepping"
        }
        if (from < min || to > max || from > to) {
            throw new IllegalArgumentException("cron field '" + spec + "': out of range " + min + "-" + max);
        }
        for (int v = from; v <= to; v += step) set.set(v);
    }

    private static int parseInt(String s, String spec) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("cron field '" + spec + "': '" + s + "' is not a number");
        }
    }

    /**
     * The next fire time strictly after {@code afterMillis}, in epoch millis. Throws if no match
     * exists within roughly five years (e.g. {@code 0 0 30 2 *}).
     */
    public long next(long afterMillis) {
        ZonedDateTime t = Instant.ofEpochMilli(afterMillis).atZone(ZoneOffset.UTC)
                .truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        ZonedDateTime limit = t.plusYears(5);
        while (t.isBefore(limit)) {
            if (!months.get(t.getMonthValue())) {
                t = t.plusMonths(1).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
            } else if (!dayMatches(t)) {
                t = t.plusDays(1).truncatedTo(ChronoUnit.DAYS);
            } else if (!hours.get(t.getHour())) {
                t = t.plusHours(1).truncatedTo(ChronoUnit.HOURS);
            } else if (!minutes.get(t.getMinute())) {
                t = t.plusMinutes(1);
            } else {
                return t.toInstant().toEpochMilli();
            }
        }
        throw new IllegalStateException("cron '" + expression + "' never fires");
    }

    /** Vixie rule: both day fields restricted -> OR; otherwise the restricted one decides. */
    private boolean dayMatches(ZonedDateTime t) {
        boolean dom = daysOfMonth.get(t.getDayOfMonth());
        boolean dow = daysOfWeek.get(t.getDayOfWeek().getValue() % 7);   // java MON=1..SUN=7 -> cron SUN=0
        if (domRestricted && dowRestricted) return dom || dow;
        if (domRestricted) return dom;
        if (dowRestricted) return dow;
        return true;
    }

    public String expression() {
        return expression;
    }
}
