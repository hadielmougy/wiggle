package com.wiggle.server.store;

import com.wiggle.core.InstanceStatus;
import com.wiggle.server.store.Rows.Instance;

import java.util.List;
import java.util.Optional;

/**
 * Workflow instances: the reads, which a read-only connection such as a read replica can serve.
 * Held on an instance shard.
 */
public interface InstanceReads {

    Optional<Instance> findInstance(String id);

    List<Instance> listInstances(String workflow, InstanceStatus status, int limit);

    /** Instances started with {@code correlationId} (a business key), newest first. */
    List<Instance> findByCorrelation(String correlationId, int limit);

    int countInstances(InstanceStatus status);

    /** Instances whose parent token belongs to {@code parentInstanceId} -- its sub-workflows. */
    List<String> childInstanceIds(String parentInstanceId);
}
