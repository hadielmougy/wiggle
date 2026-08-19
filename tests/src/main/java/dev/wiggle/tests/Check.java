package dev.wiggle.tests;

import java.util.Objects;
import java.util.function.BooleanSupplier;

/** Assertions with no test-framework dependency, so the suite runs anywhere a JVM does. */
public final class Check {

    private Check() {}

    public static void isTrue(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    public static void equal(Object actual, Object expected, String what) {
        if (!Objects.equals(actual, expected)) {
            throw new AssertionError(what + ": expected <" + expected + "> but was <" + actual + ">");
        }
    }

    public static void notNull(Object value, String what) {
        if (value == null) throw new AssertionError(what + " was null");
    }

    public static void contains(String haystack, String needle, String what) {
        if (haystack == null || !haystack.contains(needle)) {
            throw new AssertionError(what + ": <" + haystack + "> does not contain <" + needle + ">");
        }
    }

    /** Polls until the condition holds, so tests never depend on a fixed sleep. */
    public static void eventually(String what, long timeoutMillis, BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            sleep(20);
        }
        throw new AssertionError("timed out after " + timeoutMillis + "ms waiting for " + what);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted");
        }
    }
}
