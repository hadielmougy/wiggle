(ns wiggle.dashboard.actions
  "Side-effecting bridges between the API and the state atom: load-* fetch and store, the
   verbs (cancel, signal, schedule) act and then refresh what they touched."
  (:require [wiggle.dashboard.api :as api]
            [wiggle.dashboard.state :as st :refer [db]]))

(declare load-graph!)

(defn- store! [k] (fn [v] (swap! db assoc k v)))

(defn load-auth! []
  (-> (api/auth) (.then (store! :auth)) (.catch (fn [_] nil))))

(defn load-cluster! []
  (-> (api/cluster) (.then (store! :cluster)) (.catch (fn [_] nil))))

(defn load-workflows! []
  (-> (api/workflows)
      (.then #(swap! db assoc :workflows (:workflows %)))
      (.catch (fn [_] nil))))

(defn load-instances! []
  (-> (api/instances (:filter @db))
      (.then #(swap! db assoc :instances (:instances %)))
      (.catch st/on-error)))

(defn load-signals! []
  (-> (api/signals)
      (.then #(swap! db assoc :signals (:signals %)))
      (.catch st/on-error)))

(defn load-backlog! []
  (-> (api/backlog)
      (.then #(swap! db assoc :backlog %))
      (.catch st/on-error)))

(defn load-schedules! []
  (-> (api/schedules)
      (.then #(swap! db assoc :schedules (:schedules %)))
      (.catch st/on-error)))

(def ^:private window-millis
  {"15m" (* 15 60000) "1h" 3600000 "24h" 86400000 "7d" (* 7 86400000) "all" nil})

(defn load-stats!
  "Per-step durations of the performance tab's workflow over its window, plus that workflow's
   graph for the heat map. Nothing to ask for until a workflow is chosen."
  []
  (let [{:keys [workflow window]} (:perf @db)
        span (get window-millis window 3600000)
        since (when span (- (js/Date.now) span))]
    (if (empty? workflow)
      (swap! db assoc :stats nil)
      (do
        (when (not= workflow (:graph-for @db)) (load-graph! workflow))
        (-> (api/stats workflow since)
            (.then #(swap! db assoc :stats %))
            (.catch st/on-error))))))

(defn load-anomalies! []
  (-> (api/anomalies (get-in @db [:perf :workflow]) 200)
      (.then #(swap! db assoc :anomalies (:anomalies %)))
      (.catch st/on-error)))

(defn load-perf! []
  (load-stats!)
  (load-anomalies!))

(defn load-graph! [name]
  (-> (api/workflow-graph name)
      (.then (fn [g] (swap! db assoc :graph g :graph-for name)))
      (.catch st/on-error)))

(defn load-detail! [id]
  (swap! db assoc :selected id)
  (-> (api/instance id)
      (.then (fn [d]
               (swap! db assoc :detail d)
               ;; make sure the graph for this workflow is loaded for the trace overlay
               (let [wf (get-in d [:instance :workflow])]
                 (when (not= wf (:graph-for @db)) (load-graph! wf)))))
      (.catch st/on-error)))

(defn cancel! [id reason]
  (-> (api/cancel-instance id reason)
      (.then (fn [_] (st/toast! :ok "instance cancelled") (load-instances!) (load-detail! id)))
      (.catch st/on-error)))

(defn signal! [instance-id signal payload]
  (-> (api/deliver-signal instance-id signal payload)
      (.then (fn [_]
               (st/toast! :ok (str "signal '" signal "' delivered"))
               (load-signals!) (load-instances!)
               (when (= instance-id (:selected @db)) (load-detail! instance-id))))
      (.catch st/on-error)))

(defn create-schedule! [body]
  (-> (api/create-schedule body)
      (.then (fn [_] (st/toast! :ok "schedule created") (load-schedules!)))
      (.catch st/on-error)))

(defn delete-schedule! [id]
  (-> (api/delete-schedule id)
      (.then (fn [_] (st/toast! :ok "schedule deleted") (load-schedules!)))
      (.catch st/on-error)))
