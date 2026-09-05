package com.wiggle.order;

import com.wiggle.client.WiggleConnection;
import com.wiggle.core.Json;

/** Prints cluster membership as JSON. Usage: ClusterStatus [target] (default localhost:8080). */
public final class ClusterStatus {

    public static void main(String[] args) {
        String target = args.length > 0 ? args[0] : "localhost:8080";
        try (WiggleConnection wiggle = WiggleConnection.direct(target)) {
            System.out.println(Json.write(wiggle.client().cluster()));
        }
    }
}
