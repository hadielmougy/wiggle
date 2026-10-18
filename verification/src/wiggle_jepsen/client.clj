(ns wiggle-jepsen.client
  "A Jepsen client that drives Wiggle over its Java client as a read/write register per
   workflow instance:

     [:w k v]  -> signal instance(k) with {\"v\": v}  (the payload merges into the context)
     [:r k _]  -> read instance(k).context()[\"v\"]

   One instance is pre-started per key at setup, so keys are reused and transactions can
   interfere. In coordinator mode each op resolves the owning cell for its instance id
   (exercising T12 routing) and records it as :cell, which the single-cell checker asserts
   never changes for a given key."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen.client :as client])
  (:import (com.google.protobuf ByteString)
           (dev.wiggle.client WiggleClient)
           (dev.wiggle.client.dsl Workflow)
           (dev.wiggle.core Json)
           (dev.wiggle.proto CellCoordinatorGrpc RegisterWorkflowRequest ResolveRequest)
           (io.grpc Grpc InsecureChannelCredentials ManagedChannel)
           (java.util HashMap)))

(def ^:const namespace-name "jepsen")
(def ^:const workflow-name  "jepsen-reg")
;; The awaitSignal chain length -> the max writes one instance accepts before it terminates.
;; Reads on a terminal instance still return its final context, so a long chain just means a
;; long-running test can keep writing.
(def ^:const writes-per-key 2000)

(defn- define-workflow
  "A workflow that is a chain of awaitSignal(\"set\") nodes: each signal merges {\"v\": ...}
   into the instance context (a register write), and the flow parks waiting for the next.
   SEAM: confirm this validates/terminates as you expect for your DSL version; swap in a
   looping/append workflow if you move to an elle.list-append workload."
  []
  (loop [s (Workflow/define workflow-name), i 0]
    (if (< i writes-per-key)
      (recur (.awaitSignal s "set") (inc i))
      (.build s))))

(defn- coord-stub [^ManagedChannel chan]
  (CellCoordinatorGrpc/newBlockingStub chan))

(defn- resolve-cell
  "Resolves the cell endpoint that owns instance-id via the coordinator (T12 routing)."
  [stub instance-id]
  (-> stub
      (.resolve (-> (ResolveRequest/newBuilder) (.setInstanceId instance-id) (.build)))
      .getEndpoint .getTarget))

(defn- resolve-namespace
  "Resolves a cell endpoint that hosts new instances of the namespace (a new-start route)."
  [stub]
  (-> stub
      (.resolve (-> (ResolveRequest/newBuilder) (.setNamespace namespace-name) (.build)))
      .getEndpoint .getTarget))

(defn- register-fanout!
  "Registers the workflow across every cell of the namespace via the coordinator (R23 fan-out)."
  [stub bp]
  (let [json (Json/write (.toJson (.definition bp)))]
    (.registerWorkflow stub (-> (RegisterWorkflowRequest/newBuilder)
                                (.setNamespace namespace-name)
                                (.setName workflow-name)
                                (.setDefinition (ByteString/copyFromUtf8 json))
                                (.build)))))

(defn- client-for
  "Returns [WiggleClient cell] for an instance id. Direct mode dials the fixed node; coordinator
   mode resolves the owning cell and caches a client per endpoint."
  [{:keys [coordinator stub node clients]} instance-id]
  (if coordinator
    (let [cell (resolve-cell stub instance-id)]
      [(get (swap! clients update cell #(or % (WiggleClient. cell))) cell) cell])
    [(get (swap! clients update node #(or % (WiggleClient. node))) node) node]))

(defn- read-v [^WiggleClient wc id]
  (let [ctx (.context (.instance wc id))]
    (when (instance? java.util.Map ctx)
      (some-> (get ctx "v") str Long/parseLong))))   ; JSON-safe: values are written as longs

(defn- indeterminate? [^Throwable e]
  (let [m (str (.getMessage e))]
    (boolean (some #(re-find % m) [#"(?i)deadline" #"(?i)unavailable" #"(?i)timeout"]))))

(defrecord Client [coordinator ids clients node stub chan]
  client/Client
  (open! [this test node]
    (let [chan (when coordinator
                 (.build (Grpc/newChannelBuilder coordinator (InsecureChannelCredentials/create))))]
      (assoc this
             :node node
             :chan chan
             :stub (when chan (coord-stub chan))
             :clients (atom {}))))

  (setup! [this test]
    ;; Register the workflow and pre-start one instance per key -- once, guarded by the shared ids
    ;; atom so concurrent clients don't double-register. Coordinator mode registers via the
    ;; coordinator's fan-out and starts on a resolved cell; direct mode goes straight to the node.
    (when (compare-and-set! ids nil {})
      (let [bp (define-workflow)]
        (if coordinator
          (do (register-fanout! stub bp)
              (reset! ids (into {} (for [k (:keyspace test)]
                                     (let [wc (WiggleClient. (resolve-namespace stub))]
                                       (try [k (.start wc workflow-name (HashMap.))]
                                            (finally (.close wc))))))))
          (let [wc (WiggleClient. node)]
            (try
              (.register wc bp)
              (reset! ids (into {} (for [k (:keyspace test)]
                                     [k (.start wc workflow-name (HashMap.))])))
              (finally (.close wc)))))
        (info "provisioned" (count @ids) "register instances")))
    this)

  (invoke! [this test op]
    (let [[[f k v]] (:value op)
          id        (get @ids k)]
      (try
        (let [[^WiggleClient wc cell] (client-for this id)]
          (case f
            :w (do (.signal wc id "set" (doto (HashMap.) (.put "v" v)))
                   (assoc op :type :ok :cell cell))
            :r (assoc op :type :ok :cell cell :value [[:r k (read-v wc id)]])))
        (catch Exception e
          (assoc op :type (if (indeterminate? e) :info :fail) :error (.getMessage e))))))

  (teardown! [this test])   ; external cluster: leave instances/cluster running

  (close! [this test]
    (doseq [^WiggleClient wc (vals @(:clients this))] (.close wc))
    (when-let [^ManagedChannel c (:chan this)] (.shutdownNow c))))

(defn make-client [coordinator ids]
  (map->Client {:coordinator coordinator :ids ids}))
