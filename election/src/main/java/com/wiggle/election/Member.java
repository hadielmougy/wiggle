package com.wiggle.election;

/**
 * One process in an election roster.
 *
 * <p>{@code firstHeartbeat} is what decides the leader, so it must be the moment the process
 * <em>first</em> announced itself and must not move on later heartbeats -- a node that keeps
 * refreshing it can never win, and a node that resets it on reconnect would cause a needless
 * handover. {@code lastHeartbeat} is what decides liveness and moves every beat.
 *
 * @param id            unique per process, stable for its lifetime
 * @param name          human-readable, for logs and status; not used by the election
 * @param firstHeartbeat when this process announced itself (epoch millis)
 * @param lastHeartbeat  when it last checked in (epoch millis)
 */
public record Member(String id, String name, long firstHeartbeat, long lastHeartbeat) {

    public Member withHeartbeat(long now) {
        return new Member(id, name, firstHeartbeat, now);
    }
}
