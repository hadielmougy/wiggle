package com.wiggle.core;

/**
 * A domain event a handler emitted while its step ran, on its way to the
 * {@linkplain EventView event log}. The server appends it in the transaction that settles the
 * step, so an attempt that threw leaves none behind and its retry emits fresh.
 *
 * <p>{@code type} is the consumer-facing name of the fact; the {@code wf.} prefix is reserved
 * for the engine's own lifecycle entries. {@code payload} is a JSON object -- a record or a map
 * -- versioned by whatever convention the emitter and its consumers share.
 */
public record EmittedEvent(String type, Object payload) {

    /** The prefix an emitted type may not take: it is how a consumer tells the engine's own entries. */
    public static final String RESERVED_PREFIX = "wf.";

    public EmittedEvent {
        if (type == null || type.isBlank()) {
            throw new IllegalArgumentException("an event type is required: it is what a consumer routes on");
        }
        if (type.startsWith(RESERVED_PREFIX)) {
            throw new IllegalArgumentException("event type '" + type + "' takes the reserved \""
                    + RESERVED_PREFIX + "\" prefix, which marks the engine's own lifecycle entries");
        }
    }
}
