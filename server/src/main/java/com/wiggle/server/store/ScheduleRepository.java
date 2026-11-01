package com.wiggle.server.store;

import java.util.List;

/**
 * Schedules: the writes, and the reads that must see the primary. Held on the home shard.
 */
public interface ScheduleRepository extends ScheduleReads {

    void putSchedule(Rows.Schedule schedule);

    void deleteSchedule(String id);

    /** Schedules whose fire time has passed. */
    List<Rows.Schedule> dueSchedules(long now, int max);

    /**
     * Advances a schedule's fire time iff it still reads {@code expectedFireAt} -- the
     * compare-and-set that keeps overlapping leaders from double-firing.
     */
    boolean claimSchedule(String id, long expectedFireAt, long nextFireAt);
}
