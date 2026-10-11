package com.wiggle.core.criteria;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A condition on a task: a comparison, or comparisons joined by {@code and}, {@code or} and
 * {@code not}. Built with {@link CriteriaBuilder}; immutable, with value equality.
 */
public sealed interface Predicate permits Predicate.Comparison, Predicate.And, Predicate.Or, Predicate.Not {

    default Predicate not() {
        return new Not(this);
    }

    /** {@code args} are literals: a String for a TEXT field, a Long for an INTEGER one. */
    record Comparison(Field field, Op op, List<Object> args) implements Predicate {
        public Comparison {
            Objects.requireNonNull(field, "field");
            Objects.requireNonNull(op, "op");
            args = args == null ? List.of() : List.copyOf(args);
        }
    }

    record And(List<Predicate> terms) implements Predicate {
        public And {
            terms = List.copyOf(terms);
        }
    }

    record Or(List<Predicate> terms) implements Predicate {
        public Or {
            terms = List.copyOf(terms);
        }
    }

    record Not(Predicate term) implements Predicate {
        public Not {
            Objects.requireNonNull(term, "term");
        }
    }

    /** Every comparison in the tree, in order. */
    default List<Comparison> comparisons() {
        List<Comparison> out = new ArrayList<>();
        collect(this, out);
        return out;
    }

    private static void collect(Predicate p, List<Comparison> out) {
        switch (p) {
            case Comparison c -> out.add(c);
            case And a -> a.terms().forEach(t -> collect(t, out));
            case Or o -> o.terms().forEach(t -> collect(t, out));
            case Not n -> collect(n.term(), out);
        }
    }
}
