package com.wiggle.server.store;

import java.util.List;

/**
 * Schedules: the reads, which a read-only connection such as a read replica can serve. Held on the
 * home shard.
 */
public interface ScheduleReads {

    List<Rows.Schedule> schedules();

    /** The schedule for a workflow, if one exists -- workflow is a unique key for schedules. */
    java.util.Optional<Rows.Schedule> scheduleByWorkflow(String workflow);
}
