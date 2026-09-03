package com.wiggle.client.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Binds a combine method's parameter to the pre-fork context (the shared context as it was when the
 * fork fanned out, without the per-branch results), decoded into the parameter's type. Optional --
 * a combine that only needs its branch outputs takes just {@link Arm} parameters. See
 * {@link Handlers}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Context {
}
