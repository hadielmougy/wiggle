package com.wiggle.server.engine;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Runs a leader sweep's due items, each through its own action (its own transaction), up to
 * {@code parallelism} at once. One item's failure is logged and leaves the rest running; items
 * finish in no particular order. A parallelism of 1 runs them one after another, in list order.
 */
final class Sweeper {

    private static final System.Logger LOG = System.getLogger(Sweeper.class.getName());

    private final int parallelism;

    Sweeper(int parallelism) {
        this.parallelism = Math.max(1, parallelism);
    }

    /**
     * Applies {@code action} to every item and returns how many it answered true for. An action
     * that throws counts as false; {@code label} names the item in the warning.
     */
    <T> int run(List<T> items, Function<T, String> label, Predicate<T> action) {
        if (parallelism == 1 || items.size() <= 1) {
            int done = 0;
            for (T item : items) {
                if (attempt(item, label, action)) done++;
            }
            return done;
        }
        AtomicInteger done = new AtomicInteger();
        Semaphore slots = new Semaphore(parallelism);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (T item : items) {
                if (Thread.currentThread().isInterrupted()) break;
                slots.acquireUninterruptibly();
                pool.submit(() -> {
                    try {
                        if (attempt(item, label, action)) done.incrementAndGet();
                    } finally {
                        slots.release();
                    }
                });
            }
        }
        return done.get();
    }

    private static <T> boolean attempt(T item, Function<T, String> label, Predicate<T> action) {
        try {
            return action.test(item);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, label.apply(item) + " failed: " + e);
            return false;
        }
    }
}
