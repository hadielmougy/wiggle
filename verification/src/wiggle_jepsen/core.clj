(ns wiggle-jepsen.core
  "Jepsen test for the Wiggle workflow engine + cell coordinator.

   Workload: a read/write register per workflow instance (see wiggle-jepsen.client). Checked with
   Elle (rw-register) for register anomalies, plus custom checkers for Wiggle's own invariants
   (single-cell routing, liveness). Faults: coordinator epoch churn (drain/retire) + network
   partitions (see wiggle-jepsen.nemesis).

   Run (external cluster already up):
     lein run test --node 127.0.0.1:8081 --node 127.0.0.1:8082 --coordinator 127.0.0.1:8099 \\
                   --time-limit 120 --concurrency 20
   Direct (single cluster, no coordinator): omit --coordinator.

   STATUS: scaffold. The structure, Elle wiring, checkers, and coordinator nemesis are complete;
   the cluster lifecycle is EXTERNAL (see wiggle-jepsen.db) and node kill/pause is a documented
   extension. Not yet executed end-to-end -- see README.md."
  (:require [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
                    [os :as os]
                    [tests :as tests]]
            [wiggle-jepsen [client :as client]
                           [nemesis :as nem]
                           [checkers :as wc]
                           [db :as db]]))

(def keyspace (vec (range 8)))            ; small + reused so transactions actually interfere
(defonce ^:private write-counter (atom 0)) ; globally unique write values (Elle needs uniqueness)

(defn- r [_ _] {:type :invoke :f :txn :value [[:r (rand-nth keyspace) nil]]})
(defn- w [_ _] {:type :invoke :f :txn :value [[:w (rand-nth keyspace) (swap! write-counter inc)]]})

(defn- final-reads
  "One read per key, per client, tagged :final? for the liveness checker."
  []
  (gen/each-thread (mapv (fn [k] {:type :invoke :f :txn :final? true :value [[:r k nil]]}) keyspace)))

(defn wiggle-test [opts]
  (merge tests/noop-test opts
         {:name      (str "wiggle-" (:workload opts "register"))
          :keyspace  keyspace
          :os        os/noop
          :db        (db/external-cluster)
          :client    (client/make-client (:coordinator opts) (atom nil))
          :nemesis   (nem/full opts)
          :generator (gen/phases
                       (->> (gen/mix [r w])
                            (gen/stagger 1/50)
                            (gen/nemesis (nem/schedule opts))
                            (gen/time-limit (:time-limit opts 120)))
                       (gen/nemesis (nem/final-heal))
                       (gen/sleep 5)                 ; let the cluster settle after the last fault
                       (gen/clients (final-reads)))
          :checker   (checker/compose
                       {:elle        (wc/elle-register)
                        :single-cell (wc/single-cell)
                        :liveness    (wc/liveness)
                        :perf        (checker/perf)})}))

(def opt-spec
  [[nil "--coordinator URL" "Coordinator gRPC host:port; omit for direct (single-cluster) mode."]
   [nil "--workload NAME" "Workload (currently only 'register')." :default "register"]])

(defn -main [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn wiggle-test :opt-spec opt-spec})
                   (cli/serve-cmd))
            args))
