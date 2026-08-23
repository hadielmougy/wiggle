(ns wiggle.dashboard.core
  (:require [reagent.core :as r]
            [reagent.dom :as rdom]
            [wiggle.dashboard.state :as st :refer [db]]
            [wiggle.dashboard.actions :as act]
            [wiggle.dashboard.views :as views]))

(defn- refresh-tab!
  "Reloads just what the active tab shows, plus the open instance detail. Runs on the poll
   timer only when auto-refresh is on."
  []
  (case (st/tab)
    :instances (do (act/load-instances!)
                   (when (:selected @db) (act/load-detail! (:selected @db))))
    :signals   (act/load-signals!)
    :schedules (act/load-schedules!)
    :workflows nil)) ; a graph is static once loaded

(defn- tick! []
  (act/load-cluster!)
  (when (:auto? @db) (refresh-tab!)))

(defonce ^:private timers (atom nil))

(defn start-polling! []
  (when-not @timers
    (reset! timers [(js/setInterval tick! 2000)
                    (js/setInterval act/load-workflows! 5000)])))

(defn mount! []
  (rdom/render [views/app] (.getElementById js/document "app")))

(defn init []
  ;; initial load
  (act/load-cluster!)
  (act/load-workflows!)
  (act/load-instances!)
  (act/load-signals!)
  (act/load-schedules!)
  ;; when the tab changes, load its data immediately rather than waiting for the timer
  (add-watch db ::tab-change
             (fn [_ _ old new]
               (when (not= (:tab old) (:tab new)) (refresh-tab!))))
  (start-polling!)
  (mount!))

;; hot-reload entry: re-render on recompile
(defn ^:dev/after-load reload! []
  (mount!))
