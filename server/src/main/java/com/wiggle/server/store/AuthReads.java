package com.wiggle.server.store;

import java.util.List;
import java.util.Optional;

/**
 * Portal accounts, roles, sessions, machine credentials and the auth audit: the reads, which a
 * read-only connection such as a read replica can serve. Held on the auth shard.
 */
public interface AuthReads {

    /** A portal account. Held on the auth shard. */
    Optional<Rows.AuthUser> findAuthUser(String name);

    /** Every portal account, oldest first. Held on the auth shard. */
    List<Rows.AuthUser> authUsers();

    /** The names of the roles {@code user} holds, sorted. Held on the auth shard. */
    List<String> authRolesOf(String user);

    /** Every role, by name. Held on the auth shard. */
    List<Rows.AuthRole> authRoles();

    /** The session whose token hashes to {@code idHash}, expired or not. Held on the auth shard. */
    Optional<Rows.AuthSession> findAuthSession(String idHash);

    /** Up to {@code max} audit entries after {@code afterSeq}, oldest first. Held on the auth shard. */
    List<Rows.AuthAudit> authAuditAfter(long afterSeq, int max);

    /** The highest audit seq, or 0 when there is none. Held on the auth shard. */
    long authAuditHead();

    /** Every machine credential, by id. Held on the auth shard. */
    List<Rows.AuthCredential> authCredentials();

    /** The API key credential whose key hashes to {@code keyHash}. Held on the auth shard. */
    Optional<Rows.AuthCredential> findAuthCredentialByKeyHash(String keyHash);

    /** The certificate credential for {@code subject}. Held on the auth shard. */
    Optional<Rows.AuthCredential> findAuthCredentialBySubject(String subject);

    /** Whether any audit entry records {@code action}. Held on the auth shard. */
    boolean authAuditHas(String action);
}
