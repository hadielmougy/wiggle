package com.wiggle.client.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Binds a combine method's parameter to a fork branch by name: the branch's accumulated result is
 * decoded into the parameter's type. The name is the {@link com.wiggle.client.dsl.Branch#name()
 * branch name} used in the {@code fork}. See {@link Handlers}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Arm {
    String value();
}
