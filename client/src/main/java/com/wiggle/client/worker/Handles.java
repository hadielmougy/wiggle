package com.wiggle.client.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Names the graph node a handler method serves when the method's own name can't (or shouldn't)
 * match it. By default a method binds the step whose name matches its own, case/style-insensitively
 * ({@code inStock} ↔ {@code in-stock}); {@code @Handles} overrides that:
 *
 * <pre>{@code
 * @Handles("capture-payment")
 * public Order doCapture(Order o) { ... }            // serves the step "capture-payment"
 *
 * @Handles("in-stock")
 * public GateActivity<Order> stockGate() { ... }     // a factory method, renamed the same way
 * }</pre>
 *
 * Works on plain handler methods and on {@linkplain Activity typed-activity factory methods}
 * alike. The value is matched under the same canonical folding as method names.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Handles {
    /** The graph node name this method serves. */
    String value();
}
