package com.wiggle.jdbc;

import com.wiggle.core.criteria.Field;
import com.wiggle.core.criteria.Op;
import com.wiggle.core.criteria.Predicate;
import com.wiggle.core.criteria.Predicate.And;
import com.wiggle.core.criteria.Predicate.Comparison;
import com.wiggle.core.criteria.Predicate.Not;
import com.wiggle.core.criteria.Predicate.Or;
import org.jdbi.v3.core.statement.SqlStatement;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A criteria predicate as a condition over {@code wf_token}. Fields map to fixed columns and every
 * literal is a named bind. Each comparison is guarded by {@code IS NOT NULL}, or is an
 * {@code EXISTS} probe of {@code wf_token_attr} for an attribute, so the condition is never NULL and
 * {@code NOT} keeps the two-valued meaning of {@link com.wiggle.core.criteria.Criteria}.
 */
record CriteriaSql(String sql, Map<String, Object> binds) {

    static CriteriaSql compile(Predicate p, Dialect dialect) {
        Compiler c = new Compiler(dialect);
        c.emit(p);
        return new CriteriaSql(c.sql.toString(), Map.copyOf(c.binds));
    }

    void bind(SqlStatement<?> q) {
        binds.forEach(q::bind);
    }

    /** An attribute literal as its column stores it: numbers as DECIMAL, booleans as text. */
    private static Object attributeLiteral(Object v) {
        return switch (v) {
            case Long n -> BigDecimal.valueOf(n);
            case Boolean b -> b.toString();
            default -> v;
        };
    }

    private static final class Compiler {
        private final Dialect dialect;
        private final StringBuilder sql = new StringBuilder();
        private final Map<String, Object> binds = new LinkedHashMap<>();

        Compiler(Dialect dialect) {
            this.dialect = dialect;
        }

        void emit(Predicate p) {
            switch (p) {
                case Comparison c -> comparison(c);
                case And a -> junction(" AND ", a.terms());
                case Or o -> junction(" OR ", o.terms());
                case Not n -> {
                    sql.append("(NOT ");
                    emit(n.term());
                    sql.append(')');
                }
            }
        }

        private void junction(String op, List<Predicate> terms) {
            sql.append('(');
            for (int i = 0; i < terms.size(); i++) {
                if (i > 0) sql.append(op);
                emit(terms.get(i));
            }
            sql.append(')');
        }

        private void comparison(Comparison c) {
            if (c.field() == Field.STEP) {
                step(c);
                return;
            }
            if (c.field() == Field.ATTRIBUTE) {
                attribute(c);
                return;
            }
            test(c, column(c.field()), c.field().type() == Field.Type.TEXT);
        }

        /** {@code col} under {@code c}'s operator, guarded so that an absent value tests false. */
        private void test(Comparison c, String col, boolean text) {
            switch (c.op()) {
                case IS_NULL -> sql.append('(').append(col).append(" IS NULL)");
                case IS_NOT_NULL -> sql.append('(').append(col).append(" IS NOT NULL)");
                default -> {
                    sql.append('(').append(col).append(" IS NOT NULL AND ");
                    String ordered = c.op().ordering() && text ? dialect.codePointOrder(col) : col;
                    List<Object> args = c.args();
                    switch (c.op()) {
                        case EQUAL -> sql.append(col).append(" = ").append(bind(args.getFirst()));
                        case NOT_EQUAL -> sql.append(col).append(" <> ").append(bind(args.getFirst()));
                        case IN -> sql.append(col).append(" IN (").append(bindAll(args, "")).append(')');
                        case LESS_THAN -> sql.append(ordered).append(" < ").append(bind(args.getFirst()));
                        case LESS_THAN_OR_EQUAL -> sql.append(ordered).append(" <= ").append(bind(args.getFirst()));
                        case GREATER_THAN -> sql.append(ordered).append(" > ").append(bind(args.getFirst()));
                        case GREATER_THAN_OR_EQUAL -> sql.append(ordered).append(" >= ").append(bind(args.getFirst()));
                        case BETWEEN -> sql.append(ordered).append(" BETWEEN ").append(bind(args.get(0)))
                                .append(" AND ").append(bind(args.get(1)));
                        case LIKE -> sql.append(col).append(" LIKE ").append(bind(args.getFirst())).append(" ESCAPE '\\'");
                        case IS_NULL, IS_NOT_NULL -> throw new IllegalStateException("handled above");
                    }
                    sql.append(')');
                }
            }
        }

        /**
         * A declared attribute is a row of {@code wf_token_attr}, present or not. A comparison holds
         * when the row exists, has the literal's type, and its value passes the operator.
         */
        private void attribute(Comparison c) {
            String probe = "SELECT 1 FROM wf_token_attr qa WHERE qa.token_id = wf_token.id AND qa.name = "
                    + bind(c.attribute());
            switch (c.op()) {
                case IS_NULL -> sql.append("(NOT EXISTS (").append(probe).append("))");
                case IS_NOT_NULL -> sql.append("(EXISTS (").append(probe).append("))");
                default -> {
                    Object first = c.args().getFirst();
                    String tag = first instanceof Boolean ? "B" : first instanceof String ? "S" : "N";
                    String col = first instanceof Number ? "qa.n_val" : "qa.s_val";
                    sql.append("(EXISTS (").append(probe).append(" AND qa.t = '").append(tag).append("' AND ");
                    Comparison typed = new Comparison(c.field(), c.attribute(), c.op(),
                            c.args().stream().map(CriteriaSql::attributeLiteral).toList());
                    test(typed, col, first instanceof String);
                    sql.append("))");
                }
            }
        }

        /** A step name is the part of {@code activity} after {@code workflow#}. */
        private void step(Comparison c) {
            String named = "workflow || '#' || ";
            sql.append("(activity IS NOT NULL AND ");
            switch (c.op()) {
                case EQUAL -> sql.append("activity = ").append(named).append(bind(c.args().getFirst()));
                case NOT_EQUAL -> sql.append("activity <> ").append(named).append(bind(c.args().getFirst()));
                case IN -> sql.append("activity IN (").append(bindAll(c.args(), named)).append(')');
                default -> throw new IllegalArgumentException("STEP does not support " + c.op());
            }
            sql.append(')');
        }

        private String bindAll(List<Object> args, String each) {
            StringBuilder out = new StringBuilder();
            for (Object a : args) {
                if (!out.isEmpty()) out.append(',');
                out.append(each).append(bind(a));
            }
            return out.toString();
        }

        private String bind(Object value) {
            String name = "crit" + binds.size();
            binds.put(name, value);
            return ":" + name;
        }

        private static String column(Field f) {
            return switch (f) {
                case WORKFLOW -> "workflow";
                case VERSION -> "version";
                case QUEUE -> "queue";
                case NODE_ID -> "node_id";
                case KIND -> "kind";
                case ATTEMPT -> "(attempt + 1)";
                case CORRELATION_ID -> "correlation_id";
                case INSTANCE_CREATED_AT -> "inst_created_at";
                case STEP -> throw new IllegalStateException("STEP compiles against activity");
                case ATTRIBUTE -> throw new IllegalStateException("an attribute compiles to a probe of wf_token_attr");
            };
        }
    }
}
