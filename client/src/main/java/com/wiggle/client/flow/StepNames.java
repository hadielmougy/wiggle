package com.wiggle.client.flow;

import com.wiggle.client.worker.Handles;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.Method;

/**
 * Reads the node name off a method reference. This is the whole bridge between the typed
 * {@link WiggleFuture} API and the name-keyed topology the server and worker already speak: a
 * reference like {@code account::withdraw} yields the string {@code "withdraw"}, which is emitted as
 * the node name and later bound back to that method by the worker's canonical folding (so
 * {@code inStock} and a node named {@code in-stock} are the same step).
 *
 * <p>Mechanism: the flow interfaces extend {@link Serializable}, so javac emits a {@code
 * writeReplace} on the lambda class that returns a {@link SerializedLambda} naming the implementation
 * method. Nothing is invoked -- the reference is read, not called.
 *
 * <p>Two things are rejected here rather than left to fail confusingly later:
 * <ul>
 *   <li><b>lambdas</b> ({@code o -> account.withdraw(o)}), whose implementation is a synthetic
 *       {@code lambda$...} method with no handler to bind to on the worker;</li>
 *   <li><b>captured values</b> ({@code () -> account.withdraw(amount)}), because a captured local is
 *       not part of the topology: it would not survive registration, replay or a restart. A step's
 *       input is the persisted context, never a value from the defining JVM.</li>
 * </ul>
 *
 * <p>When the referenced method carries {@link Handles @Handles}, that name wins -- so a method
 * renamed for the graph is renamed on
 * both sides at once.
 */
final class StepNames {

    private StepNames() {}

    /** The node name a method reference stands for. */
    static String of(Serializable methodRef) {
        SerializedLambda lambda = serializedForm(methodRef);
        String impl = lambda.getImplMethodName();

        if (impl.startsWith("lambda$")) {
            throw new IllegalArgumentException(
                    "a flow step must be a direct method reference (account::withdraw), not a lambda: "
                    + "a lambda body has no handler method for a worker to bind. If the step needs a "
                    + "different node name, annotate the handler method with @Handles(\"...\").");
        }
        if (lambda.getCapturedArgCount() > 1) {
            throw new IllegalArgumentException(
                    "the method reference for '" + impl + "' captures " + (lambda.getCapturedArgCount() - 1)
                    + " value(s) from the enclosing scope. A step's input is the workflow context, "
                    + "decoded into its parameter -- captured values are not part of the topology and "
                    + "would not survive registration or replay.");
        }
        return handlesOverride(methodRef, lambda, impl);
    }

    /** The {@link Handles} name on the referenced method, if it has one; otherwise the method's name. */
    private static String handlesOverride(Serializable methodRef, SerializedLambda lambda, String impl) {
        Method m = resolve(methodRef, lambda, impl);
        if (m != null) {
            Handles handles = m.getAnnotation(Handles.class);
            if (handles != null) return handles.value();
        }
        return impl;
    }

    /** The implementation method, or null when its class cannot be loaded (the name alone still works). */
    private static Method resolve(Serializable methodRef, SerializedLambda lambda, String impl) {
        try {
            ClassLoader loader = methodRef.getClass().getClassLoader();
            Class<?> owner = Class.forName(lambda.getImplClass().replace('/', '.'), false, loader);
            int params = parameterCount(lambda.getImplMethodSignature());
            for (Method m : owner.getDeclaredMethods()) {
                if (m.getName().equals(impl) && m.getParameterCount() == params) return m;
            }
            return null;
        } catch (ClassNotFoundException | LinkageError e) {
            return null;
        }
    }

    private static SerializedLambda serializedForm(Serializable methodRef) {
        try {
            Method writeReplace = methodRef.getClass().getDeclaredMethod("writeReplace");
            writeReplace.setAccessible(true);
            Object form = writeReplace.invoke(methodRef);
            if (!(form instanceof SerializedLambda lambda)) {
                throw new IllegalArgumentException("not a method reference: " + methodRef.getClass());
            }
            return lambda;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "a flow step must be a method reference to a handler method (account::withdraw); "
                    + methodRef.getClass().getName() + " is not one", e);
        } catch (InaccessibleObjectException e) {
            throw new IllegalStateException(
                    "cannot read the method reference in " + methodRef.getClass().getName()
                    + ": the module defining the flow must open its package to com.wiggle.client "
                    + "(add 'opens <your.package>;' to its module-info), or define the flow with the "
                    + "name-based DSL in com.wiggle.client.dsl instead", e);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot read the method reference in "
                    + methodRef.getClass().getName(), e);
        }
    }

    /** Number of parameters in a JVM method descriptor, e.g. {@code (Lx/Order;I)Lx/Payment;} -> 2. */
    static int parameterCount(String descriptor) {
        int count = 0;
        int i = descriptor.indexOf('(') + 1;
        while (i < descriptor.length() && descriptor.charAt(i) != ')') {
            while (descriptor.charAt(i) == '[') i++;
            if (descriptor.charAt(i) == 'L') i = descriptor.indexOf(';', i) + 1;
            else i++;
            count++;
        }
        return count;
    }
}