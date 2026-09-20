package com.wiggle.server.engine;

import com.wiggle.server.store.Tx;

public record RunningModeContext(String taskId, String leaseOwner, Object result, Tx tx, long loopMaxIterations) {

}
