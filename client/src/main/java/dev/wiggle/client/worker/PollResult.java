package dev.wiggle.client.worker;

import dev.wiggle.core.TaskActivation;

import java.util.List;

/**
 * The outcome of a poll: the leased tasks (possibly empty), plus a server backpressure hint.
 * When {@code retryAfterMillis > 0} the server is shedding load and the worker should wait that
 * long before polling again instead of retrying immediately (the value already carries jitter).
 */
public record PollResult(List<TaskActivation> tasks, long retryAfterMillis) { }
