package com.wiggle.jdbc;

import com.wiggle.server.auth.Scope;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.SearchRepository;
import com.wiggle.server.store.SearchText;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** {@link SearchRepository} on {@code wf_search_doc} and {@code wf_search_attr}. */
final class JdbcSearchRepository extends JdbcRepository implements SearchRepository {

    private static final String SEARCH_COLUMNS =
            "instance_id,workflow,version,status,correlation_id,text,created_at,updated_at";

    JdbcSearchRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public boolean upsertSearchDoc(Rows.SearchDoc d) {
        String update = "UPDATE wf_search_doc SET workflow=:workflow,version=:version,status=:status,"
                + "correlation_id=:correlationId,text=:text,created_at=:created,updated_at=:updated "
                + "WHERE instance_id=:id AND updated_at<=:updated";
        boolean wrote = jdbc().update(update, searchDoc(d)) > 0
                || jdbc().update(dialect.insertIgnore("INSERT INTO wf_search_doc "
                        + "(instance_id,workflow,version,status,correlation_id,text,created_at,updated_at) VALUES "
                        + "(:id,:workflow,:version,:status,:correlationId,:text,:created,:updated)"), searchDoc(d)) > 0
                || jdbc().update(update, searchDoc(d)) > 0;   // another writer inserted it first
        if (wrote) writeSearchAttributes(d);
        return wrote;
    }

    /** Replaces the document's attribute rows with its current ones. */
    private void writeSearchAttributes(Rows.SearchDoc d) {
        jdbc().update("DELETE FROM wf_search_attr WHERE instance_id=:id", params().text("id", d.instanceId()));
        if (d.attributes().isEmpty()) return;
        List<SqlParameterSource> rows = new ArrayList<>(d.attributes().size());
        for (Map.Entry<String, Object> a : d.attributes().entrySet()) {
            Object v = a.getValue();
            rows.add(params()
                    .text("id", d.instanceId())
                    .text("workflow", d.workflow())
                    .text("name", a.getKey())
                    .text("t", v instanceof Boolean ? "B" : v instanceof String ? "S" : "N")
                    .text("s", v instanceof Number ? null : String.valueOf(v))
                    .decimal("n", v instanceof Number n ? attributeNumber(n) : null));
        }
        jdbc().batchUpdate("INSERT INTO wf_search_attr (instance_id,workflow,name,t,s_val,n_val)"
                + " VALUES (:id,:workflow,:name,:t,:s,:n)", rows.toArray(SqlParameterSource[]::new));
    }

    private static Params searchDoc(Rows.SearchDoc d) {
        return params()
                .text("id", d.instanceId())
                .text("workflow", d.workflow())
                .add("version", d.version())
                .text("status", d.status())
                .text("correlationId", d.correlationId())
                .text("text", d.text())
                .add("created", d.createdAt())
                .add("updated", d.updatedAt());
    }

    @Override public void deleteSearchDoc(String instanceId) {
        jdbc().update("DELETE FROM wf_search_attr WHERE instance_id=:id", params().text("id", instanceId));
        jdbc().update("DELETE FROM wf_search_doc WHERE instance_id=:id", params().text("id", instanceId));
    }

    /** {@code scope} on column {@code column}, for a WHERE that already has a condition; bound by {@link #bindWorkflows}. */
    private static String workflowFilter(Scope scope, String column) {
        if (scope.all()) return "";
        if (scope.isEmpty()) return " AND 1=0";
        List<String> terms = new ArrayList<>();
        if (!scope.names().isEmpty()) terms.add(column + " IN (:workflows)");
        for (int i = 0; i < scope.prefixes().size(); i++) terms.add(column + " LIKE :wfp" + i + " ESCAPE '!'");
        return " AND (" + String.join(" OR ", terms) + ")";
    }

    private static void bindWorkflows(Params p, Scope scope) {
        if (scope.all()) return;
        if (!scope.names().isEmpty()) p.add("workflows", List.copyOf(scope.names()));
        int i = 0;
        for (String prefix : scope.prefixes()) p.text("wfp" + i++, likePrefix(prefix));
    }

    /** A LIKE pattern matching what starts with {@code prefix}, escaped with {@code !}. */
    static String likePrefix(String prefix) {
        StringBuilder out = new StringBuilder(prefix.length() + 1);
        for (char c : prefix.toCharArray()) {
            if (c == '!' || c == '%' || c == '_' || c == '[') out.append('!');
            out.append(c);
        }
        return out.append('%').toString();
    }

    @Override public int deleteSearchDocsBefore(long updatedBefore, int max) {
        List<String> ids = jdbc().queryForList("SELECT instance_id FROM wf_search_doc WHERE updated_at<:before "
                + "ORDER BY updated_at LIMIT :max", params().add("before", updatedBefore).add("max", max), String.class);
        if (ids.isEmpty()) return 0;
        jdbc().update("DELETE FROM wf_search_attr WHERE instance_id IN (:ids)", params().add("ids", ids));
        return jdbc().update("DELETE FROM wf_search_doc WHERE instance_id IN (:ids)", params().add("ids", ids));
    }

    @Override public List<Rows.SearchDoc> searchDocsAfter(String afterId, int max) {
        List<Rows.SearchDoc> docs = jdbc().query("SELECT " + SEARCH_COLUMNS + " FROM wf_search_doc "
                        + "WHERE instance_id>:after ORDER BY instance_id LIMIT :max",
                params().text("after", afterId == null ? "" : afterId).add("max", max), (rs, row) -> searchDoc(rs));
        return withAttributes(docs);
    }

    /** {@code docs} with the search attributes stored for them, one statement for all. */
    private List<Rows.SearchDoc> withAttributes(List<Rows.SearchDoc> docs) {
        if (docs.isEmpty()) return docs;
        Map<String, Map<String, Object>> attributes = new HashMap<>();
        jdbc().query("SELECT instance_id,name,t,s_val,n_val FROM wf_search_attr WHERE instance_id IN (:ids)",
                params().add("ids", docs.stream().map(Rows.SearchDoc::instanceId).toList()),
                rs -> {
                    String t = rs.getString("t");
                    Object v = switch (t) {
                        case "B" -> Boolean.parseBoolean(rs.getString("s_val"));
                        case "S" -> rs.getString("s_val");
                        default -> attributeValue(rs.getBigDecimal("n_val"));
                    };
                    attributes.computeIfAbsent(rs.getString("instance_id"), k -> new HashMap<>())
                            .put(rs.getString("name"), v);
                });
        return docs.stream().map(d -> new Rows.SearchDoc(d.instanceId(), d.workflow(), d.version(), d.status(),
                d.correlationId(), d.text(), d.createdAt(), d.updatedAt(),
                attributes.getOrDefault(d.instanceId(), Map.of()))).toList();
    }

    /** A stored number as the type it was declared with: a Long when it has no fraction, else the decimal. */
    private static Object attributeValue(BigDecimal n) {
        BigDecimal s = n.stripTrailingZeros();
        return s.scale() <= 0 && s.precision() - s.scale() <= 18 ? (Object) s.longValueExact() : s;
    }

    private static Rows.SearchDoc searchDoc(ResultSet rs) throws SQLException {
        return new Rows.SearchDoc(rs.getString("instance_id"), rs.getString("workflow"), rs.getInt("version"),
                rs.getString("status"), rs.getString("correlation_id"), rs.getString("text"),
                rs.getLong("created_at"), rs.getLong("updated_at"));
    }

    @Override public List<Rows.SearchHit> searchDocs(Rows.SearchQuery q) {
        boolean text = q.text() != null && !q.text().isBlank();
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        where.append(workflowFilter(q.workflows(), "workflow"));
        if (q.status() != null) where.append(" AND status=:status");
        if (q.from() != null) where.append(" AND updated_at>=:from");
        if (q.to() != null) where.append(" AND updated_at<=:to");
        for (int i = 0; i < q.attributes().size(); i++) where.append(attributeFilter(q.attributes().get(i), i));
        Params p = params();
        String sql;
        if (dialect.supportsFullText()) {
            sql = text
                    ? "SELECT " + SEARCH_COLUMNS + ", ts_rank(tsv, plainto_tsquery('simple', :text)) AS score "
                      + "FROM wf_search_doc" + where + " AND tsv @@ plainto_tsquery('simple', :text) "
                      + "ORDER BY score DESC, updated_at DESC, instance_id LIMIT :limit"
                    : "SELECT " + SEARCH_COLUMNS + ", 0 AS score FROM wf_search_doc" + where
                      + " ORDER BY updated_at DESC, instance_id LIMIT :limit";
            p.add("limit", q.limit());
            if (text) p.text("text", q.text());
        } else {
            sql = "SELECT " + SEARCH_COLUMNS + ", 0 AS score FROM wf_search_doc" + where
                    + " ORDER BY updated_at DESC, instance_id";
        }
        bindWorkflows(p, q.workflows());
        if (q.status() != null) p.text("status", q.status());
        if (q.from() != null) p.add("from", q.from());
        if (q.to() != null) p.add("to", q.to());
        if (!q.attributes().isEmpty()) p.text("aw", q.attributeWorkflow());
        for (int i = 0; i < q.attributes().size(); i++) {
            Rows.AttrFilter f = q.attributes().get(i);
            p.text("an" + i, f.name());
            if (f.value() instanceof Number n) p.decimal("av" + i, attributeNumber(n));
            else p.text("av" + i, String.valueOf(f.value()));
        }
        List<Rows.SearchHit> hits = dialect.supportsFullText()
                ? jdbc().query(sql, p, (rs, row) -> new Rows.SearchHit(searchDoc(rs), rs.getDouble("score")))
                : SearchText.match(jdbc().query(sql, p, (rs, row) -> searchDoc(rs)), q.text(), q.limit());
        List<Rows.SearchDoc> docs = withAttributes(hits.stream().map(Rows.SearchHit::doc).toList());
        List<Rows.SearchHit> out = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) out.add(new Rows.SearchHit(docs.get(i), hits.get(i).score()));
        return out;
    }

    /** The condition that the document has attribute {@code f}, as a semi-join on the value index. */
    private static String attributeFilter(Rows.AttrFilter f, int i) {
        String test = f.value() instanceof Number
                ? "a.t='N' AND a.n_val" + switch (f.op()) {
                    case EQUAL -> "=";
                    case LESS_THAN -> "<";
                    case LESS_THAN_OR_EQUAL -> "<=";
                    case GREATER_THAN -> ">";
                    case GREATER_THAN_OR_EQUAL -> ">=";
                    default -> throw new IllegalStateException("unexpected operator " + f.op());
                } + ":av" + i
                : "a.t='" + (f.value() instanceof Boolean ? "B" : "S") + "' AND a.s_val=:av" + i;
        return " AND EXISTS (SELECT 1 FROM wf_search_attr a WHERE a.instance_id=wf_search_doc.instance_id"
                + " AND a.workflow=:aw AND a.name=:an" + i + " AND " + test + ")";
    }
}
