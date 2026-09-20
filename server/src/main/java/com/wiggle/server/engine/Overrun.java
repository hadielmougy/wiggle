package com.wiggle.server.engine;

/**
 * Whether a step ran past a budget the instance cannot exceed, and why. A node kind that has no
 * budget to blow returns {@link #NONE}; anything else fails the instance with {@link #message()}.
 */
record Overrun(boolean exceeded, String message) {

    static final Overrun NONE = new Overrun(false, null);

    static Overrun of(String message) {
        return new Overrun(true, message);
    }
}
