package com.wiggle.observe;

import com.wiggle.client.worker.Compensation;

/** The snapshot pair an undo is given, built by the application from what it kept. */
public final class Compensations {

    private Compensations() {}

    public static <A, B> Compensation<A, B> of(A input, B result) {
        return new Snapshot<>(input, result);
    }

    private record Snapshot<A, B>(A input, B result) implements Compensation<A, B> {}
}
