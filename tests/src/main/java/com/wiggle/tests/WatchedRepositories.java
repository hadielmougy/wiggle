package com.wiggle.tests;

import com.wiggle.server.store.WriteRepositories;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/** Repositories that report each call to a {@link Watcher} once it returns, for tests that count what the engine asks the store for. */
public final class WatchedRepositories {

    private WatchedRepositories() {}

    /** Told of one repository call: the method, its arguments, and what it returned. */
    @FunctionalInterface
    public interface Watcher {
        void called(Method method, Object[] args, Object result);
    }

    /** {@code repositories}, with every call on any of its areas reported to {@code watcher}. */
    public static WriteRepositories watch(WriteRepositories repositories, Watcher watcher) {
        return proxy(WriteRepositories.class, repositories, (method, args, out) -> {
            if (method.getParameterCount() != 0 || !method.getReturnType().isInterface()) return out;
            Class<?> area = WriteRepositories.class.getMethod(method.getName()).getReturnType();
            return proxy(area, out, (m, a, result) -> {
                watcher.called(m, a, result);
                return result;
            });
        });
    }

    private interface After {
        Object apply(Method method, Object[] args, Object out) throws Exception;
    }

    private static <T> T proxy(Class<T> type, Object target, After after) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, (proxy, method, args) -> {
            Object out;
            try {
                out = method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
            return after.apply(method, args, out);
        }));
    }
}
