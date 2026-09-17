package com.wiggle.server.engine;

import com.wiggle.server.store.Storage;
import com.wiggle.server.store.Tx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Batches worker-side mutations into shared transactions. A caller parks until the batch carrying
 * its mutation commits, so an acknowledged operation is exactly as durable as on the per-operation
 * path -- there are just fewer commits. The first mutation of a batch arms a linger; the batch
 * flushes when the linger expires or {@code maxBatch} is reached.
 *
 * <p>A batch that fails is rolled back whole and replayed one operation per transaction, each ack
 * then carrying its own outcome. That fallback needs rollback to be real, which is why
 * {@link Storage#transactional()} gates constructing this at all.
 */
final class GroupCommit {

    private static final System.Logger LOG = System.getLogger(GroupCommit.class.getName());

    private record Pending(Function<Tx, Object> body, CompletableFuture<Object> ack) {}

    private final Storage storage;
    private final DispatchNotifier notifier;
    private final ThreadLocal<Set<String>> readyQueues;
    private final long lingerNanos;
    private final int maxBatch;
    private final LinkedBlockingQueue<Pending> queue = new LinkedBlockingQueue<>();

    GroupCommit(Storage storage, DispatchNotifier notifier, ThreadLocal<Set<String>> readyQueues,
                long lingerMillis, int maxBatch) {
        this.storage = storage;
        this.notifier = notifier;
        this.readyQueues = readyQueues;
        this.lingerNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(0, lingerMillis));
        this.maxBatch = Math.max(1, maxBatch);
        Thread t = new Thread(this::loop, "wiggle-group-commit");
        t.setDaemon(true);
        t.start();
    }

    /** Runs {@code body} in the next batch and returns its result after that batch commits. */
    @SuppressWarnings("unchecked")
    <T> T run(Function<Tx, T> body) {
        CompletableFuture<Object> ack = new CompletableFuture<>();
        queue.add(new Pending((Function<Tx, Object>) body, ack));
        try {
            return (T) ack.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException re) throw re;
            if (cause instanceof Error err) throw err;
            throw e;
        }
    }

    private void loop() {
        while (true) {
            try {
                Pending first = queue.take();
                List<Pending> batch = new ArrayList<>(Math.min(maxBatch, 16));
                batch.add(first);
                long deadline = System.nanoTime() + lingerNanos;
                while (batch.size() < maxBatch) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) break;
                    Pending next = queue.poll(remaining, TimeUnit.NANOSECONDS);
                    if (next == null) break;
                    batch.add(next);
                }
                flush(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                LOG.log(System.Logger.Level.ERROR, "group-commit loop error", t);
            }
        }
    }

    /** One transaction for the whole batch. Acks complete only after it commits. */
    private void flush(List<Pending> batch) {
        Object[] results = new Object[batch.size()];
        Set<String> ready = new HashSet<>();
        readyQueues.set(ready);
        try {
            storage.inTx(tx -> {
                for (int i = 0; i < batch.size(); i++) results[i] = batch.get(i).body().apply(tx);
                return null;
            });
        } catch (Throwable batchFailure) {
            readyQueues.remove();
            LOG.log(System.Logger.Level.DEBUG, () -> "batch of " + batch.size() + " rolled back ("
                    + batchFailure + "); replaying individually");
            replay(batch);
            return;
        }
        readyQueues.remove();
        notifier.signal(ready);
        for (int i = 0; i < batch.size(); i++) batch.get(i).ack().complete(results[i]);
    }

    /** The rolled-back batch, one transaction per operation -- as if it had never been batched. */
    private void replay(List<Pending> batch) {
        for (Pending p : batch) {
            Set<String> ready = new HashSet<>();
            readyQueues.set(ready);
            try {
                Object result = storage.inTx(p.body());
                readyQueues.remove();
                notifier.signal(ready);
                p.ack().complete(result);
            } catch (Throwable t) {
                readyQueues.remove();
                p.ack().completeExceptionally(t);
            }
        }
    }
}
