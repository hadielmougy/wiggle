package com.wiggle.server.store;

import java.util.List;

/**
 * Portal accounts, roles, sessions, machine credentials and the auth audit: the writes, and the
 * reads that must see the primary. Held on the auth shard.
 */
public interface AuthRepository extends AuthReads {

    /** Writes a portal account, replacing any of that name. Held on the auth shard. */
    void putAuthUser(Rows.AuthUser user);

    /** Deletes a portal account with its role grants and sessions; false when there was none. */
    boolean deleteAuthUser(String name);

    /** Replaces the roles {@code user} holds. */
    void setAuthRolesOf(String user, List<String> roles);

    /** Writes a role, replacing any of that name. Held on the auth shard. */
    void putAuthRole(Rows.AuthRole role);

    /** Deletes a role and every grant of it; false when there was none. */
    boolean deleteAuthRole(String name);

    /** Adds a machine credential; its id, key hash and subject are each unique. */
    void insertAuthCredential(Rows.AuthCredential credential);

    /** Deletes a machine credential; false when there was none. */
    boolean deleteAuthCredential(String id);

    void insertAuthSession(Rows.AuthSession session);

    void deleteAuthSession(String idHash);

    /** Deletes every session of {@code user} except {@code keepIdHash} (null keeps none); returns how many. */
    int deleteAuthSessionsOf(String user, String keepIdHash);

    /** Deletes up to {@code max} sessions that expired before {@code now}; returns how many. */
    int deleteExpiredAuthSessions(long now, int max);

    /** Appends to the auth audit and returns the seq the store assigned. */
    long appendAuthAudit(Rows.AuthAudit entry);
}
