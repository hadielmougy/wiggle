package com.wiggle.tutorial;

import com.wiggle.client.CoordinatedConnection;
import com.wiggle.client.WiggleClient;
import com.wiggle.client.WiggleConnection;
import com.wiggle.client.flow.FlowSpec;
import com.wiggle.client.worker.Worker;
import com.wiggle.placement.IdCodec;
import com.wiggle.core.InstanceView;
import com.wiggle.core.Tls;
import com.wiggle.proto.RingSlot;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

/**
 * Tutorial 3: coordinator, cells, and a database each. The coordinator owns placement -- which cell
 * holds which shard of which namespace -- and the client asks it where to go.
 *
 * <p>See {@link Orders} for the flow; the flow itself is identical in all three tutorials, which is
 * the point: the deployment shape is not a property of the workflow.
 */
public final class Coordinated {

    // docs:begin open-epoch
    /**
     * A coordinated namespace is placed by an explicit ring and nothing else: until an epoch names a
     * cell, that cell is on standby and mints no ids. This is the one operator step with no default.
     */
    public static void placeNamespace(CoordinatedConnection wiggle, String namespace, String cellId) {
        wiggle.openEpoch(namespace, List.of(
                RingSlot.newBuilder().setShard(0).setCellId(cellId).build()));
    }
    // docs:end open-epoch

    // docs:begin main
    public static void main(String[] args) throws Exception {
        try (CoordinatedConnection wiggle =
                     WiggleConnection.coordinator("localhost:8099", Tls.Options.DISABLED, "eu-west")) {

            placeNamespace(wiggle, "orders", "cell-a");        // once, at provisioning time

            WiggleClient client = wiggle.clientForNamespace("orders");   // routed to the owning cell
            FlowSpec orders = Orders.spec();
            client.register(orders);

            try (Worker worker = new Worker(client, "worker-1")
                    .registerHandler(new OrderHandlers())
                    .start()) {

                String id = client.start(orders, new Orders.Order("A-1001",
                        List.of(new Orders.Item("PEN", new BigDecimal("2.50")),
                                new Orders.Item("PAD", new BigDecimal("4.00"))),
                        BigDecimal.ZERO, "NEW"));

                // the id carries its own namespace, epoch and shard -- it is self-routing
                IdCodec.Placement p = IdCodec.parse(id).orElseThrow();
                System.out.println("started " + id + " in " + p.namespace() + " epoch " + p.epoch());

                InstanceView done = wiggle.clientForInstance(id)
                        .awaitCompletion(id, Duration.ofSeconds(30));
                System.out.println(done.status() + " " + done.context());
            }
        }
    }
    // docs:end main

    private Coordinated() {}
}
