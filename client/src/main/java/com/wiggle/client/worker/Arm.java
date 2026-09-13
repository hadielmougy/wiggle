package com.wiggle.client.worker;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Binds a combine method's parameter to a fork branch by name: the branch's accumulated result is
 * decoded into the parameter's type. The name is the {@link com.wiggle.client.flow.Branch#name()
 * branch name} used in the {@code fork}. See {@link Handlers}.
 *
 * <pre>{@code
 * Order merge(@Context Order base, @Arm("payment") Order payment, @Arm("shipping") Order shipping)
 * }</pre>
 *
 * <p><b>Optional.</b> A combine whose parameters carry no {@code @Arm} at all takes the arms
 * <b>by position</b> instead -- parameter order is fork order:
 *
 * <pre>{@code
 * Order merge(@Context Order base, Order payment, Order shipping)   // fork(payment, shipping)
 * }</pre>
 *
 * Positional binding must take every arm, and {@link Context @Context} still needs its annotation --
 * it is what distinguishes the pre-fork context from an arm. Naming the arms is the safer form, and
 * the one to prefer when several arms share a type: nothing then tells two parameters apart but their
 * order, and swapping them compiles and runs. Naming also lets a merge take the arms in any order, or
 * ignore the ones it does not need. The two forms cannot be mixed in one method.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Arm {
    String value();
}
