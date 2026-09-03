package com.wiggle.client.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method in a {@link Handlers} class as a custom decoder rather than a step: it takes the
 * raw persisted JSON (a {@code Map}) and returns a typed context object. Its return type is the type
 * it decodes; whenever a step or combine parameter of that type is bound, this runs instead of the
 * default reflective mapping. This is the seam for schema versioning / upcasting an older context to
 * the current shape, or for a bespoke codec.
 *
 * <pre>{@code
 * @Decode Order load(Map<String,Object> raw) {  // upcast v1/v2 -> current Order
 *     ...
 * }
 * }</pre>
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Decode {
}
