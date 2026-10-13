package com.wiggle.server.engine;

import com.wiggle.server.store.ClaimFilter;

record PollRequest(
        String workerId,
        ClaimFilter filter,
        int max,
        long deadline,
        Cancellation cancelled
) {}
