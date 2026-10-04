(ns wiggle.dashboard.state
  "The whole UI state in one reagent atom, with a couple of derived helpers. Deliberately a
   minimal store rather than re-frame: the dashboard is small and this keeps the dependency
   surface tiny."
  (:require [reagent.core :as r]
            [clojure.string :as str]))

(defonce db
  (r/atom
   {:tab       :instances          ; :instances | :workflows | :schedules | :signals | :backlog | :performance | :users
    :auth      nil                 ; {:required bool :user ".."} — drives the logout button
    :cluster   nil
    :workflows []
    :instances []
    :signals   []
    :backlog   nil                 ; {:slices .. :uncoveredSlices .. :strandedTasks ..}
    :schedules []
    :users     []                  ; console accounts, built-ins first (admins only)
    :stats     nil                 ; {:workflow .. :nodes [..]} per-step durations, slowest p95 first
    :perf      {:workflow "" :window "1h"}   ; what the performance tab shows
    :filter    {:workflow "" :status "" :limit 100
                :search "" :search-by :correlation}   ; free-text lookup by :correlation | :id
    :selected  nil                 ; selected instance id
    :detail    nil                 ; {:instance .. :tokens ..}
    :graph     nil                 ; {:name .. :nodes .. } — step names and the workflows tab's step table
    :graph-for nil                 ; which workflow the loaded graph is for
    :wf-open   nil                 ; the workflow expanded on the workflows tab
    :auto?     true
    :window    nil                 ; {:kind :detail|:password :mode :normal|:max|:min} — the floating popup
    :toast     nil}))              ; {:kind :ok|:err :text ".."}

(defn tab [] (:tab @db))
(defn set-tab! [t] (swap! db assoc :tab t :window nil))   ; switching tabs dismisses any popup

;; ---- floating window (the instance detail / password popup) ----
(defn open-window! [kind] (swap! db assoc :window {:kind kind :mode :normal}))
(defn close-window! [] (swap! db assoc :window nil :selected nil :detail nil))
;; toggle back to :normal if already in that mode, so the same button restores
(defn toggle-window-mode! [mode] (swap! db update-in [:window :mode] #(if (= % mode) :normal mode)))

(defn toast! [kind text]
  (swap! db assoc :toast {:kind kind :text text})
  (js/setTimeout #(swap! db assoc :toast nil) 3500))

(defn on-error [e]
  (toast! :err (or (ex-message e) (str e))))

(defn set-filter! [k v] (swap! db assoc-in [:filter k] v))
(defn set-perf! [k v] (swap! db assoc-in [:perf k] v))

;; ---- authorization: the permissions the server says this session holds ----
(defn can?
  "Whether this session may do `action`, on `scope` when given, else on at least one scope.
   True until the server has answered, so nothing flickers away on load."
  ([action] (can? action nil))
  ([action scope]
   (let [ps (get-in @db [:auth :permissions])]
     (or (nil? ps)
         (boolean (some #(or (= % "*") (= % action)
                             (if scope (= % (str action ":" scope)) (str/starts-with? % (str action ":"))))
                        ps))))))

;; ---- accounts: managed on the auth shard, by whoever holds user.manage ----
(defn manages-users? [] (boolean (get-in @db [:auth :managesUsers])))
(defn can-manage-users? [] (and (manages-users?) (can? "user.manage")))
;; A built-in account's password comes from the environment, so the console cannot change it.
(defn can-change-password? [] (boolean (get-in @db [:auth :canChangePassword])))
