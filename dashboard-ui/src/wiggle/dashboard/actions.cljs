(ns wiggle.dashboard.actions
  "Side-effecting bridges between the API and the state atom: load-* fetch and store, the
   verbs (cancel, signal, schedule) act and then refresh what they touched."
  (:require [wiggle.dashboard.api :as api]
            [wiggle.dashboard.state :as st :refer [db]]))

(defn- store! [k] (fn [v] (swap! db assoc k v)))

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

(defn load-schedules! []
  (-> (api/schedules)
      (.then #(swap! db assoc :schedules (:schedules %)))
      (.catch st/on-error)))

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
