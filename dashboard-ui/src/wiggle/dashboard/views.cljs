(ns wiggle.dashboard.views
  (:require [reagent.core :as r]
            [clojure.string :as str]
            [wiggle.dashboard.state :as st :refer [db]]
            [wiggle.dashboard.actions :as act]
            [wiggle.dashboard.diagram :as diagram]
            [wiggle.dashboard.util :as u]))

;; ---------------------------------------------------------------- shared bits

(defn badge [status]
  [:span {:class (str "badge " status)} status])

(def ^:private status-priority
  {"FAILED" 5 "RUNNING" 4 "READY" 4 "AWAITING" 3 "WAITING" 3
   "CANCELLED" 2 "DONE" 1 "JOINED" 1 "COMPLETED" 1})

(defn token-statuses
  "Collapses an instance's tokens to one status per node for the trace overlay, keeping the
   most interesting status when a node has several tokens (retries, dynamic branches)."
  [tokens]
  (reduce (fn [m {:keys [nodeId status]}]
            (if (and nodeId
                     (> (get status-priority status 0)
                        (get status-priority (get m nodeId) 0)))
              (assoc m nodeId status)
              m))
          {} tokens))

;; ---------------------------------------------------------------- header

(defn cluster-view []
  (let [c (:cluster @db)]
    [:div.cluster
     (if-not c
       "connecting…"
       (for [m (:members c)]
         ^{:key (:id m)}
         [:span [:span {:class (str "dot" (cond (not (:alive m)) " dead"
                                                 (:leader m) " leader" :else ""))}]
          (:name m) (when (:leader m) " (leader)")]))]))

(defn header []
  (let [tab (st/tab)]
    [:header
     [:h1 "🌀 WIGGLE"]
     [:div.tabs
      (for [[k label] [[:instances "Instances"] [:workflows "Workflows"]
                       [:schedules "Schedules"] [:signals "Signals"]]]
        ^{:key k}
        [:button {:class (when (= k tab) "active")
                  :on-click #(st/set-tab! k)} label])]
     [:div.spacer]
     [:label.inline [:input {:type "checkbox" :checked (:auto? @db)
                             :on-change #(swap! db assoc :auto? (.. % -target -checked))}] "auto-refresh"]
     [cluster-view]]))

;; ---------------------------------------------------------------- signal form

(defn signal-form
  "Inline payload editor for delivering a signal. on-send is (fn [payload-clj])."
  [signal-name on-send]
  (let [text (r/atom "{}")]
    (fn [signal-name on-send]
      [:div.row {:style {:padding "10px 14px"}}
       [:textarea {:rows 2 :style {:flex 1} :value @text
                   :on-change #(reset! text (.. % -target -value))
                   :placeholder "payload JSON (merged into the context)"}]
       [:button.primary
        {:on-click #(let [p (u/parse-json-or @text ::bad)]
                      (if (= p ::bad)
                        (st/toast! :err "payload is not valid JSON")
                        (on-send p)))}
        "deliver '" signal-name "'"]])))

;; ---------------------------------------------------------------- detail

(defn detail-panel []
  (let [{:keys [detail selected graph graph-for]} @db
        i (:instance detail)]
    [:section.panel
     [:h2 "Detail" (when i [:span.muted {:style {:font-weight 400}}
                            " · " [:code (:id i)]])]
     (cond
       (not selected) [:div.empty "select an instance to trace it"]
       (not i)        [:div.empty "loading…"]
       :else
       (let [tokens (:tokens detail)
             statuses (token-statuses tokens)
             graph-ok (= graph-for (:workflow i))]
         [:div
          [:div.toolbar
           [badge (:status i)]
           [:span.muted (:workflow i) " v" (:version i)]
           [:div.spacer {:style {:margin-left "auto"}}]
           (when (= (:status i) "RUNNING")
             [:button.danger {:on-click #(act/cancel! (:id i) "cancelled from dashboard")} "cancel"])]

          (when (:error i) [:pre.err (:error i)])

          (when (and graph graph-ok)
            [:div
             [:h2 {:style {:padding "10px 14px 0" :margin 0 :fontSize 12 :color "var(--muted)"}} "Trace"]
             [diagram/diagram graph {:statuses statuses}]])

          ;; any signal this instance is waiting on -> inline deliver
          (for [t tokens
                :when (and (= (:kind t) "SIGNAL") (= (:status t) "AWAITING"))]
            ^{:key (:id t)}
            [:div {:style {:borderTop "1px solid var(--line)"}}
             [:div {:style {:padding "10px 14px 0" :color "var(--warn)"}}
              "waiting for signal " [:strong (:activity t)]]
             [signal-form (:activity t) #(act/signal! (:id i) (:activity t) %)]])

          [:h2 {:style {:padding "10px 14px 0" :margin 0 :fontSize 12 :color "var(--muted)"}} "Tokens"]
          (if (seq tokens)
            [:table
             [:thead [:tr [:th "node"] [:th "kind"] [:th "status"] [:th "try"] [:th "last error"]]]
             [:tbody
              (for [t tokens]
                ^{:key (:id t)}
                [:tr [:td [:code (:nodeId t)]] [:td (:kind t)]
                 [:td [badge (:status t)]] [:td (:attempt t)]
                 [:td.muted (:lastError t)]])]]
            [:div.empty "no tokens"])

          [:h2 {:style {:padding "10px 14px 0" :margin 0 :fontSize 12 :color "var(--muted)"}} "Context"]
          [:pre {:style {:margin "8px 14px 14px"}} (u/pretty-json (:context i))]]))]))

;; ---------------------------------------------------------------- instances tab

(defn instances-toolbar []
  (let [f (:filter @db)]
    [:div.toolbar
     [:select {:value (:workflow f)
               :on-change #(do (st/set-filter! :workflow (.. % -target -value)) (act/load-instances!))}
      [:option {:value ""} "all workflows"]
      (for [w (:workflows @db)] ^{:key w} [:option {:value w} w])]
     [:select {:value (:status f)
               :on-change #(do (st/set-filter! :status (.. % -target -value)) (act/load-instances!))}
      [:option {:value ""} "all statuses"]
      (for [s ["RUNNING" "COMPLETED" "FAILED" "CANCELLED"]] ^{:key s} [:option {:value s} s])]
     [:input {:type "number" :min 1 :style {:width 80} :value (:limit f) :title "limit"
              :on-change #(do (st/set-filter! :limit (js/parseInt (.. % -target -value)))
                              (act/load-instances!))}]
     [:button.ghost {:on-click act/load-instances!} "↻"]]))

(defn instances-table []
  (let [{:keys [instances selected]} @db]
    (if-not (seq instances)
      [:div.empty "no instances"]
      [:table
       [:thead [:tr [:th "id"] [:th "workflow"] [:th "status"] [:th "updated"]]]
       [:tbody
        (for [i instances]
          ^{:key (:id i)}
          [:tr {:class (when (= (:id i) selected) "sel")
                :on-click #(act/load-detail! (:id i))}
           [:td [:code (:id i)]] [:td (:workflow i)]
           [:td [badge (:status i)]] [:td.muted (u/ago (:updatedAt i)) " ago"]])]])))

(defn instances-tab []
  [:div.cols
   [:section.panel
    [:h2 "Instances" [:span.count (count (:instances @db))]]
    [instances-toolbar]
    [instances-table]]
   [detail-panel]])

;; ---------------------------------------------------------------- workflows tab

(defn workflows-tab []
  (let [{:keys [workflows graph graph-for]} @db]
    [:div.cols.wide-left
     [:section.panel
      [:h2 "Workflows" [:span.count (count workflows)]]
      (if-not (seq workflows)
        [:div.empty "no workflows registered"]
        [:table [:tbody
                 (for [w workflows]
                   ^{:key w}
                   [:tr {:class (when (= w graph-for) "sel")
                         :on-click #(act/load-graph! w)}
                    [:td w]])]])]
     [:section.panel
      [:h2 "Diagram" (when graph-for [:span.muted {:style {:font-weight 400}} " · " graph-for
                                      " v" (:version graph)])]
      (if graph
        [diagram/diagram graph {:selected nil}]
        [:div.empty "select a workflow to see its graph"])]]))

;; ---------------------------------------------------------------- schedules tab

(defn parse-every
  "Parses 30s/5m/2h/250ms into millis; nil if unparseable."
  [s]
  (when-let [[_ n unit] (re-matches #"(?i)\s*(\d+)\s*(ms|s|m|h)?\s*" s)]
    (let [n (js/parseInt n)]
      (* n (case (some-> unit str/lower-case)
             "ms" 1 "s" 1000 "m" 60000 "h" 3600000 nil 1000)))))

(defn schedule-form []
  (let [s (r/atom {:workflow "" :mode :interval :every "1h" :cron "0 * * * *" :context "{}"})]
    (fn []
      (let [{:keys [workflow mode every cron context]} @s
            wfs (:workflows @db)]
        [:section.panel
         [:h2 "New schedule"]
         [:div {:style {:padding 14}}
          [:div.field [:span "workflow"]
           [:select {:value workflow :on-change #(swap! s assoc :workflow (.. % -target -value))}
            [:option {:value ""} "choose a workflow…"]
            (for [w wfs] ^{:key w} [:option {:value w} w])]]
          [:div.field [:span "cadence"]
           [:div.row
            [:label.inline [:input {:type "radio" :name "mode" :checked (= mode :interval)
                                    :on-change #(swap! s assoc :mode :interval)}] "interval"]
            [:label.inline [:input {:type "radio" :name "mode" :checked (= mode :cron)
                                    :on-change #(swap! s assoc :mode :cron)}] "cron"]]]
          (if (= mode :interval)
            [:div.field [:span "every (e.g. 30s, 5m, 1h, 250ms)"]
             [:input {:value every :on-change #(swap! s assoc :every (.. % -target -value))}]]
            [:div.field [:span "cron (min hour dom mon dow, UTC)"]
             [:input {:value cron :on-change #(swap! s assoc :cron (.. % -target -value))}]])
          [:div.field [:span "seed context (JSON)"]
           [:textarea {:rows 3 :value context :on-change #(swap! s assoc :context (.. % -target -value))}]]
          [:div.row
           [:button.primary
            {:on-click
             (fn []
               (let [ctx (u/parse-json-or context ::bad)]
                 (cond
                   (empty? workflow) (st/toast! :err "choose a workflow")
                   (= ctx ::bad)     (st/toast! :err "context is not valid JSON")
                   :else
                   (let [base {:workflow workflow :context ctx}
                         body (if (= mode :cron)
                                (assoc base :cron cron)
                                (assoc base :everyMillis (parse-every every)))]
                     (if (nil? (:everyMillis body ::x))
                       (st/toast! :err "could not parse the interval")
                       (act/create-schedule! body))))))}
            "create schedule"]]]]))))

(defn schedules-list []
  [:section.panel
   [:h2 "Schedules" [:span.count (count (:schedules @db))]]
   (if-not (seq (:schedules @db))
     [:div.empty "no schedules"]
     [:table
      [:thead [:tr [:th "workflow"] [:th "cadence"] [:th "next fire"] [:th ""]]]
      [:tbody
       (for [s (:schedules @db)]
         ^{:key (:id s)}
         [:tr
          [:td (:workflow s)]
          [:td (if (:cron s) [:code (:cron s)] (u/every-str (:everyMillis s)))]
          [:td.muted (u/ts (:nextFireAt s)) " " [:span.muted "(" (u/in-secs (:nextFireAt s)) ")"]]
          [:td.actions [:button.danger {:on-click #(act/delete-schedule! (:id s))} "delete"]]])]])])

(defn schedules-tab []
  [:div.cols
   [schedule-form]
   [schedules-list]])

;; ---------------------------------------------------------------- signals tab

(defn signal-row []
  (let [open (r/atom false)]
    (fn [t]
      [:<>
       [:tr
        [:td [:strong (:signal t)]] [:td (:workflow t)]
        [:td [:code (:instanceId t)]]
        [:td.muted (if (pos? (:deadline t)) (u/in-secs (:deadline t)) "—")]
        [:td.actions [:button.primary {:on-click #(swap! open not)} (if @open "close" "deliver")]]]
       (when @open
         [:tr [:td {:col-span 5 :style {:overflow "visible" :max-width "none"}}
               [signal-form (:signal t)
                #(do (act/signal! (:instanceId t) (:signal t) %) (reset! open false))]]])])))

(defn signals-tab []
  [:section.panel
   [:h2 "Pending signals" [:span.count (count (:signals @db))]]
   (if-not (seq (:signals @db))
     [:div.empty "no instances are waiting on a signal"]
     [:table
      [:thead [:tr [:th "signal"] [:th "workflow"] [:th "instance"] [:th "deadline"] [:th ""]]]
      [:tbody (for [t (:signals @db)] ^{:key (:instanceId t)} [signal-row t])]])])

;; ---------------------------------------------------------------- root

(defn toast []
  (when-let [t (:toast @db)]
    [:div {:class (str "toast " (name (:kind t)))} (:text t)]))

(defn app []
  [:div
   [header]
   [:main
    (case (st/tab)
      :instances [instances-tab]
      :workflows [workflows-tab]
      :schedules [schedules-tab]
      :signals   [signals-tab])]
   [toast]])
