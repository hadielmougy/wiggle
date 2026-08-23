(ns wiggle.dashboard.state
  "The whole UI state in one reagent atom, with a couple of derived helpers. Deliberately a
   minimal store rather than re-frame: the dashboard is small and this keeps the dependency
   surface tiny."
  (:require [reagent.core :as r]))

(defonce db
  (r/atom
   {:tab       :instances          ; :instances | :workflows | :schedules | :signals
    :cluster   nil
    :workflows []
    :instances []
    :signals   []
    :schedules []
    :filter    {:workflow "" :status "" :limit 100}
    :selected  nil                 ; selected instance id
    :detail    nil                 ; {:instance .. :tokens ..}
    :graph     nil                 ; {:name .. :nodes .. } for the diagram
    :graph-for nil                 ; which workflow the loaded graph is for
    :auto?     true
    :toast     nil}))              ; {:kind :ok|:err :text ".."}

(defn tab [] (:tab @db))
(defn set-tab! [t] (swap! db assoc :tab t))

(defn toast! [kind text]
  (swap! db assoc :toast {:kind kind :text text})
  (js/setTimeout #(swap! db assoc :toast nil) 3500))

(defn on-error [e]
  (toast! :err (or (ex-message e) (str e))))

(defn set-filter! [k v] (swap! db assoc-in [:filter k] v))
