(ns wiggle-jepsen.nemesis
  "Faults. Two kinds:

     coordinator-churn -- bumps the namespace epoch (OpenEpoch), forcing the current epoch to
                          DRAIN and a new one to open. Under load this exercises T12's
                          drain/retire and the minter/poll-set shift -- the Wiggle-specific fault.
     partition         -- Jepsen's standard random-halves network partition.

   The coordinator fault is fully implemented here; the partition uses Jepsen's built-in and
   needs net control on the nodes (SSH). Node kill/pause is left as a documented extension
   (see README) so the scaffold does not assume how your nodes are launched."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen [generator :as gen]
                    [nemesis :as nemesis]])
  (:import (dev.wiggle.proto CellCoordinatorGrpc OpenEpochRequest RingSlot)
           (io.grpc Grpc InsecureChannelCredentials)))

(defn- open-epoch!
  "Opens a new epoch for the namespace, mapping shard 0 -> cell. Idempotent enough for a fault:
   each call bumps the epoch and drains the previous one."
  [coordinator ns cell]
  (let [chan (.build (Grpc/newChannelBuilder coordinator (InsecureChannelCredentials/create)))]
    (try
      (-> (CellCoordinatorGrpc/newBlockingStub chan)
          (.openEpoch (-> (OpenEpochRequest/newBuilder)
                          (.setNamespace ns)
                          (.addRing (-> (RingSlot/newBuilder) (.setShard 0) (.setCellId cell) (.build)))
                          (.build))))
      (finally (.shutdownNow chan)))))

(defn coordinator-churn
  "Handles :bump-epoch by opening a fresh epoch on the coordinator."
  [{:keys [coordinator]}]
  (reify nemesis/Nemesis
    (setup! [this _test] this)
    (invoke! [this _test op]
      (if (= :bump-epoch (:f op))
        (do (info "nemesis: bumping epoch on" coordinator)
            (open-epoch! coordinator "jepsen" "cellA")
            (assoc op :value :bumped))
        op))
    (teardown! [this _test] this)))

(defn full
  "Compose the coordinator churn with a standard network partition. If no --coordinator is set,
   only the partition is active."
  [opts]
  (if (:coordinator opts)
    (nemesis/compose
      {#{:bump-epoch}                                  (coordinator-churn opts)
       {:start-partition :start :stop-partition :stop} (nemesis/partition-random-halves)})
    (nemesis/compose
      {{:start-partition :start :stop-partition :stop} (nemesis/partition-random-halves)})))

(defn schedule
  "A generator of nemesis ops: alternate an epoch bump and a partition window, spaced out."
  [opts]
  (let [churn (when (:coordinator opts) [{:type :info :f :bump-epoch} (gen/sleep 8)])]
    ;; An infinite lazy seq is a valid Jepsen generator; nested gen/sleep ops are handled.
    (cycle
      (concat churn
              [{:type :info :f :start-partition} (gen/sleep 8)
               {:type :info :f :stop-partition}  (gen/sleep 8)]))))

(defn final-heal
  "Make sure the network is whole before the final reads."
  []
  {:type :info :f :stop-partition})
