package com.wiggle.jdbc;

import com.wiggle.server.store.AuthRepository;
import com.wiggle.server.store.Rows;
import com.wiggle.server.store.StorageException;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** {@link AuthRepository} on the {@code wf_auth_*} tables. */
final class JdbcAuthRepository extends JdbcRepository implements AuthRepository {

    JdbcAuthRepository(JdbcStorage owner) {
        super(owner);
    }

    @Override public Optional<Rows.AuthUser> findAuthUser(String name) {
        return first(jdbc().query("SELECT * FROM wf_auth_user WHERE name=:name",
                params().text("name", name), (rs, row) -> authUser(rs)));
    }

    @Override public List<Rows.AuthUser> authUsers() {
        return jdbc().query("SELECT * FROM wf_auth_user ORDER BY created_at, name", (rs, row) -> authUser(rs));
    }

    private static Rows.AuthUser authUser(ResultSet rs) throws SQLException {
        return new Rows.AuthUser(rs.getString("name"), rs.getString("hash"), rs.getString("salt"),
                rs.getInt("iterations"), rs.getInt("disabled") != 0, rs.getLong("created_at"),
                rs.getLong("updated_at"));
    }

    @Override public List<String> authRolesOf(String user) {
        return jdbc().queryForList("SELECT role_name FROM wf_auth_user_role WHERE user_name=:user ORDER BY role_name",
                params().text("user", user), String.class);
    }

    @Override public List<Rows.AuthRole> authRoles() {
        return jdbc().query("SELECT * FROM wf_auth_role ORDER BY name",
                (rs, row) -> new Rows.AuthRole(rs.getString("name"),
                        permissionSet(rs.getString("permissions")), rs.getInt("builtin") != 0,
                        rs.getLong("created_at"), rs.getLong("updated_at")));
    }

    private static Set<String> permissionSet(String stored) {
        Set<String> out = new TreeSet<>();
        for (String p : stored.split("\\s+")) if (!p.isEmpty()) out.add(p);
        return out;
    }

    @Override public Optional<Rows.AuthSession> findAuthSession(String idHash) {
        return first(jdbc().query("SELECT * FROM wf_auth_session WHERE id_hash=:id", params().text("id", idHash),
                (rs, row) -> new Rows.AuthSession(rs.getString("id_hash"), rs.getString("user_name"),
                        rs.getLong("expires_at"), rs.getLong("created_at"))));
    }

    @Override public List<Rows.AuthAudit> authAuditAfter(long afterSeq, int max) {
        return jdbc().query("SELECT * FROM wf_auth_audit WHERE seq>:after ORDER BY seq LIMIT :max",
                params().add("after", afterSeq).add("max", max),
                (rs, row) -> new Rows.AuthAudit(rs.getLong("seq"), rs.getLong("at"), rs.getString("actor"),
                        rs.getString("action"), rs.getString("target"), rs.getString("detail")));
    }

    @Override public long authAuditHead() {
        return jdbc().queryForObject("SELECT COALESCE(MAX(seq),0) FROM wf_auth_audit", params(), Long.class);
    }

    @Override public boolean authAuditHas(String action) {
        return jdbc().queryForObject("SELECT COUNT(*) FROM wf_auth_audit WHERE action=:action",
                params().text("action", action), Long.class) > 0;
    }

    @Override public void putAuthUser(Rows.AuthUser u) {
        String update = "UPDATE wf_auth_user SET hash=:hash,salt=:salt,iterations=:iterations,"
                + "disabled=:disabled,created_at=:created,updated_at=:updated WHERE name=:name";
        if (jdbc().update(update, authUser(u)) > 0) return;
        if (jdbc().update(dialect.insertIgnore("INSERT INTO wf_auth_user "
                + "(name,hash,salt,iterations,disabled,created_at,updated_at) VALUES "
                + "(:name,:hash,:salt,:iterations,:disabled,:created,:updated)"), authUser(u)) > 0) return;
        jdbc().update(update, authUser(u));
    }

    private static Params authUser(Rows.AuthUser u) {
        return params()
                .text("name", u.name())
                .text("hash", u.hash())
                .text("salt", u.salt())
                .add("iterations", u.iterations())
                .add("disabled", u.disabled() ? 1 : 0)
                .add("created", u.createdAt())
                .add("updated", u.updatedAt());
    }

    @Override public boolean deleteAuthUser(String name) {
        jdbc().update("DELETE FROM wf_auth_user_role WHERE user_name=:name", params().text("name", name));
        jdbc().update("DELETE FROM wf_auth_session WHERE user_name=:name", params().text("name", name));
        return jdbc().update("DELETE FROM wf_auth_user WHERE name=:name", params().text("name", name)) > 0;
    }

    @Override public void setAuthRolesOf(String user, List<String> roles) {
        jdbc().update("DELETE FROM wf_auth_user_role WHERE user_name=:user", params().text("user", user));
        if (roles.isEmpty()) return;
        SqlParameterSource[] rows = new TreeSet<>(roles).stream()
                .map(r -> params().text("user", user).text("role", r))
                .toArray(SqlParameterSource[]::new);
        jdbc().batchUpdate("INSERT INTO wf_auth_user_role (user_name,role_name) VALUES (:user,:role)", rows);
    }

    @Override public void putAuthRole(Rows.AuthRole r) {
        String update = "UPDATE wf_auth_role SET permissions=:permissions,builtin=:builtin,"
                + "created_at=:created,updated_at=:updated WHERE name=:name";
        if (jdbc().update(update, authRole(r)) > 0) return;
        if (jdbc().update(dialect.insertIgnore("INSERT INTO wf_auth_role "
                + "(name,permissions,builtin,created_at,updated_at) VALUES "
                + "(:name,:permissions,:builtin,:created,:updated)"), authRole(r)) > 0) return;
        jdbc().update(update, authRole(r));
    }

    private static Params authRole(Rows.AuthRole r) {
        return params()
                .text("name", r.name())
                .text("permissions", String.join(" ", new TreeSet<>(r.permissions())))
                .add("builtin", r.builtin() ? 1 : 0)
                .add("created", r.createdAt())
                .add("updated", r.updatedAt());
    }

    @Override public boolean deleteAuthRole(String name) {
        jdbc().update("DELETE FROM wf_auth_user_role WHERE role_name=:name", params().text("name", name));
        return jdbc().update("DELETE FROM wf_auth_role WHERE name=:name", params().text("name", name)) > 0;
    }

    @Override public List<Rows.AuthCredential> authCredentials() {
        return jdbc().query("SELECT * FROM wf_auth_credential ORDER BY id", (rs, row) -> authCredential(rs));
    }

    @Override public Optional<Rows.AuthCredential> findAuthCredentialByKeyHash(String keyHash) {
        return first(jdbc().query("SELECT * FROM wf_auth_credential WHERE key_hash=:h",
                params().text("h", keyHash), (rs, row) -> authCredential(rs)));
    }

    @Override public Optional<Rows.AuthCredential> findAuthCredentialBySubject(String subject) {
        return first(jdbc().query("SELECT * FROM wf_auth_credential WHERE subject=:s",
                params().text("s", subject), (rs, row) -> authCredential(rs)));
    }

    private static Rows.AuthCredential authCredential(ResultSet rs) throws SQLException {
        long expires = rs.getLong("expires_at");
        Long expiresAt = rs.wasNull() ? null : expires;
        return new Rows.AuthCredential(rs.getString("id"), rs.getString("kind"), rs.getString("key_hash"),
                rs.getString("subject"), rs.getString("role_name"), rs.getLong("created_at"), expiresAt);
    }

    @Override public void insertAuthCredential(Rows.AuthCredential c) {
        jdbc().update("INSERT INTO wf_auth_credential (id,kind,key_hash,subject,role_name,created_at,expires_at) "
                        + "VALUES (:id,:kind,:keyHash,:subject,:role,:created,:expires)",
                params()
                        .text("id", c.id())
                        .text("kind", c.kind())
                        .text("keyHash", c.keyHash())
                        .text("subject", c.subject())
                        .text("role", c.role())
                        .add("created", c.createdAt())
                        .number("expires", c.expiresAt()));
    }

    @Override public boolean deleteAuthCredential(String id) {
        return jdbc().update("DELETE FROM wf_auth_credential WHERE id=:id", params().text("id", id)) > 0;
    }

    @Override public void insertAuthSession(Rows.AuthSession x) {
        jdbc().update("INSERT INTO wf_auth_session (id_hash,user_name,expires_at,created_at) "
                        + "VALUES (:id,:user,:expires,:created)",
                params()
                        .text("id", x.idHash())
                        .text("user", x.user())
                        .add("expires", x.expiresAt())
                        .add("created", x.createdAt()));
    }

    @Override public void deleteAuthSession(String idHash) {
        jdbc().update("DELETE FROM wf_auth_session WHERE id_hash=:id", params().text("id", idHash));
    }

    @Override public int deleteAuthSessionsOf(String user, String keepIdHash) {
        return jdbc().update("DELETE FROM wf_auth_session WHERE user_name=:user AND id_hash<>:keep",
                params().text("user", user).text("keep", keepIdHash == null ? "" : keepIdHash));
    }

    @Override public int deleteExpiredAuthSessions(long now, int max) {
        List<String> ids = jdbc().queryForList("SELECT id_hash FROM wf_auth_session WHERE expires_at<:now "
                        + "ORDER BY expires_at LIMIT :max",
                params().add("now", now).add("max", max), String.class);
        if (ids.isEmpty()) return 0;
        return jdbc().update("DELETE FROM wf_auth_session WHERE id_hash IN (:ids)", params().add("ids", ids));
    }

    @Override public long appendAuthAudit(Rows.AuthAudit e) {
        KeyHolder seq = new GeneratedKeyHolder();
        jdbc().update("INSERT INTO wf_auth_audit (at,actor,action,target,detail) "
                        + "VALUES (:at,:actor,:action,:target,:detail)",
                params()
                        .add("at", e.at())
                        .text("actor", e.actor())
                        .text("action", e.action())
                        .text("target", e.target())
                        .text("detail", e.detail()),
                seq, new String[]{"seq"});
        Number key = seq.getKey();
        if (key == null) throw new StorageException("wf_auth_audit insert returned no seq", null);
        return key.longValue();
    }

    /** The first row, as JDBI's {@code findOne} answered it for a query on a unique key. */
    static <T> Optional<T> first(List<T> rows) {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst());
    }
}
