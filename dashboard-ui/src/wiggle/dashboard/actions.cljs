(ns wiggle.dashboard.actions
  "Side-effecting bridges between the API and the state atom: load-* fetch and store, the
   verbs (cancel, signal, schedule) act and then refresh what they touched."
  (:require [clojure.string :as str]
            [wiggle.dashboard.api :as api]
            [wiggle.dashboard.state :as st :refer [db]]))

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
  (let [f (:filter @db)]
    (if (and (= :text (:search-by f)) (seq (:search f)))
      (-> (api/search-instances f)
          (.then #(swap! db assoc :instances (:hits %) :partial (:partial %)))
          (.catch st/on-error))
      (-> (api/instances f)
          (.then #(swap! db assoc :instances (:instances %) :partial false))
          (.catch st/on-error)))))

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
  "Per-step durations of the performance tab's workflow over its window. Nothing to ask for until
   a workflow is chosen."
  []
  (let [{:keys [workflow window]} (:perf @db)
        span (get window-millis window 3600000)
        since (when span (- (js/Date.now) span))]
    (if (empty? workflow)
      (swap! db assoc :stats nil)
      (-> (api/stats workflow since)
          (.then #(swap! db assoc :stats %))
          (.catch st/on-error)))))

(defn load-perf! []
  (load-stats!))

(defn load-graph! [name]
  (-> (api/workflow-graph name)
      (.then (fn [g] (swap! db assoc :graph g :graph-for name)))
      (.catch st/on-error)))

(defn load-detail! [id]
  (swap! db assoc :selected id)
  (-> (api/instance id)
      (.then (fn [d]
               (swap! db assoc :detail d)
               ;; the workflow's graph names the steps in the detail table
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

(defn load-users! []
  (when (st/can-manage-users?)
    (-> (api/users)
        (.then #(swap! db assoc :users (:users %)))
        (.catch st/on-error))
    (-> (api/roles)
        (.then #(swap! db assoc :roles (:roles %) :actions (:actions %)))
        (.catch st/on-error))
    (-> (api/audit)
        (.then #(swap! db assoc :audit (:entries %)))
        (.catch st/on-error))
    (-> (api/credentials)
        (.then #(swap! db assoc :credentials (:credentials %)))
        (.catch st/on-error))))

(defn create-credential! [body]
  (-> (api/create-credential body)
      (.then (fn [r]
               (swap! db assoc :new-key (when (:key r) {:id (:id r) :key (:key r)}))
               (st/toast! :ok (str "credential '" (:id r) "' created"))
               (load-users!)))
      (.catch st/on-error)))

(defn delete-credential! [id]
  (-> (api/delete-credential id)
      (.then (fn [_] (st/toast! :ok (str "credential '" id "' deleted")) (load-users!)))
      (.catch st/on-error)))

(defn set-roles! [name roles]
  (-> (api/set-roles name roles)
      (.then (fn [_] (st/toast! :ok (str "'" name "' now holds " (str/join ", " roles))) (load-users!)))
      (.catch st/on-error)))

(defn set-disabled! [name disabled]
  (-> (api/set-disabled name disabled)
      (.then (fn [_] (st/toast! :ok (str "'" name "' " (if disabled "disabled; signed out" "enabled")))
               (load-users!)))
      (.catch st/on-error)))

(defn put-role! [body]
  (-> (api/put-role body)
      (.then (fn [_] (st/toast! :ok (str "role '" (:name body) "' saved")) (load-users!)))
      (.catch st/on-error)))

(defn delete-role! [name]
  (-> (api/delete-role name)
      (.then (fn [_] (st/toast! :ok (str "role '" name "' deleted")) (load-users!)))
      (.catch st/on-error)))

(defn create-user! [body]
  (-> (api/create-user body)
      (.then (fn [_] (st/toast! :ok (str "user '" (:user body) "' created")) (load-auth!) (load-users!)))
      (.catch st/on-error)))

(defn delete-user! [name]
  (-> (api/delete-user name)
      (.then (fn [_] (st/toast! :ok (str "user '" name "' deleted")) (load-users!)))
      (.catch st/on-error)))

(defn reset-password! [name password]
  (-> (api/reset-password name password)
      (.then (fn [_] (st/toast! :ok (str "password reset for '" name "'; their sessions were signed out"))
               (load-users!)))
      (.catch st/on-error)))

(defn change-password! [current password on-done]
  (-> (api/change-password current password)
      (.then (fn [_] (st/toast! :ok "password changed") (when on-done (on-done))))
      (.catch st/on-error)))

(defn create-schedule! [body]
  (-> (api/create-schedule body)
      (.then (fn [_] (st/toast! :ok "schedule created") (load-schedules!)))
      (.catch st/on-error)))

(defn delete-schedule! [id]
  (-> (api/delete-schedule id)
      (.then (fn [_] (st/toast! :ok "schedule deleted") (load-schedules!)))
      (.catch st/on-error)))
