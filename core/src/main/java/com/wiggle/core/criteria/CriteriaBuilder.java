package com.wiggle.core.criteria;

import com.wiggle.core.criteria.Predicate.Comparison;

import java.util.ArrayList;
import java.util.List;

/** Builds {@link Predicate}s, after {@code jakarta.persistence.criteria.CriteriaBuilder}. */
public final class CriteriaBuilder {

    static final CriteriaBuilder INSTANCE = new CriteriaBuilder();

    private CriteriaBuilder() { }

    public <T> Predicate equal(Attribute<T> a, T value) {
        return cmp(a, Op.EQUAL, value);
    }

    public <T> Predicate notEqual(Attribute<T> a, T value) {
        return cmp(a, Op.NOT_EQUAL, value);
    }

    @SafeVarargs
    public final <T> Predicate in(Attribute<T> a, T... values) {
        List<Object> args = new ArrayList<>(values.length);
        for (T v : values) args.add(a.literal(v));
        return new Comparison(a.field(), Op.IN, args);
    }

    public <T> Predicate in(Attribute<T> a, List<T> values) {
        List<Object> args = new ArrayList<>(values.size());
        for (T v : values) args.add(a.literal(v));
        return new Comparison(a.field(), Op.IN, args);
    }

    public <T extends Comparable<? super T>> Predicate lessThan(Attribute<T> a, T value) {
        return cmp(a, Op.LESS_THAN, value);
    }

    public <T extends Comparable<? super T>> Predicate lessThanOrEqualTo(Attribute<T> a, T value) {
        return cmp(a, Op.LESS_THAN_OR_EQUAL, value);
    }

    public <T extends Comparable<? super T>> Predicate greaterThan(Attribute<T> a, T value) {
        return cmp(a, Op.GREATER_THAN, value);
    }

    public <T extends Comparable<? super T>> Predicate greaterThanOrEqualTo(Attribute<T> a, T value) {
        return cmp(a, Op.GREATER_THAN_OR_EQUAL, value);
    }

    public <T extends Comparable<? super T>> Predicate between(Attribute<T> a, T low, T high) {
        return new Comparison(a.field(), Op.BETWEEN, List.of(a.literal(low), a.literal(high)));
    }

    public Predicate like(Attribute<String> a, String pattern) {
        return cmp(a, Op.LIKE, pattern);
    }

    public Predicate isNull(Attribute<?> a) {
        return new Comparison(a.field(), Op.IS_NULL, List.of());
    }

    public Predicate isNotNull(Attribute<?> a) {
        return new Comparison(a.field(), Op.IS_NOT_NULL, List.of());
    }

    public Predicate and(Predicate... terms) {
        return new Predicate.And(List.of(terms));
    }

    public Predicate or(Predicate... terms) {
        return new Predicate.Or(List.of(terms));
    }

    public Predicate not(Predicate term) {
        return new Predicate.Not(term);
    }

    private static <T> Predicate cmp(Attribute<T> a, Op op, T value) {
        return new Comparison(a.field(), op, List.of(a.literal(value)));
    }
}
