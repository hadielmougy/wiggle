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
  (let [tab (st/tab)
        auth (:auth @db)]
    [:header
     [:h1 "🌀 WIGGLE"]
     [:div.tabs
      (for [[k label] [[:instances "Instances"] [:workflows "Workflows"]
                       [:schedules "Schedules"] [:signals "Signals"]
                       [:backlog "Backlog"] [:performance "Performance"]]]
        ^{:key k}
        [:button {:class (when (= k tab) "active")
                  :on-click #(st/set-tab! k)} label])]
     [:div.spacer]
     [:label.inline [:input {:type "checkbox" :checked (:auto? @db)
                             :on-change #(swap! db assoc :auto? (.. % -target -checked))}] "auto-refresh"]
     [cluster-view]
     (when (:required auth)
       [:div.account
        [:span.user (:user auth)]
        (when (= (:role auth) "viewer") [:span.badge.readonly {:title "read-only access"} "read-only"])
        [:button.ghost {:on-click #(set! (.. js/window -location -href) "/logout")} "Log out"]])]))

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

;; ---------------------------------------------------------------- floating window

(defn floating-window
  "A modal popup with minimize / maximize / close chrome. `opts` is {:title hiccup :on-close fn};
   the remaining args are the window body. The window's mode (:normal|:max|:min) lives in app state."
  [{:keys [title on-close]} & body]
  (let [mode (get-in @db [:window :mode] :normal)
        cls  (name mode)]
    [:div.fw-overlay {:class cls}
     [:div.fw-window {:class cls}
      [:div.fw-titlebar
       [:div.fw-title title]
       [:div.fw-controls
        [:button.fw-btn {:title "Minimize" :on-click #(st/toggle-window-mode! :min)} "—"]
        [:button.fw-btn {:title "Maximize" :on-click #(st/toggle-window-mode! :max)} "▢"]
        [:button.fw-btn.fw-close {:title "Close" :on-click on-close} "✕"]]]
      (when (not= mode :min)
        (into [:div.fw-body] body))]]))

;; ---------------------------------------------------------------- instance detail

(defn detail-body []
  (let [{:keys [detail selected graph graph-for]} @db
        i (:instance detail)]
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
           (when (and (st/can-write?) (= (:status i) "RUNNING"))
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
             (when (st/can-write?)
               [signal-form (:activity t) #(act/signal! (:id i) (:activity t) %)])])

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
          [:pre {:style {:margin "8px 14px 14px"}} (u/pretty-json (:context i))]]))))

;; ---------------------------------------------------------------- instances tab

(defn instances-toolbar []
  (let [f (:filter @db)
        searching (seq (:search f))]
    [:div.toolbar
     [:select {:value (:workflow f) :disabled (boolean searching)
               :on-change #(do (st/set-filter! :workflow (.. % -target -value)) (act/load-instances!))}
      [:option {:value ""} "all workflows"]
      (for [w (:workflows @db)] ^{:key w} [:option {:value w} w])]
     [:select {:value (:status f) :disabled (boolean searching)
               :on-change #(do (st/set-filter! :status (.. % -target -value)) (act/load-instances!))}
      [:option {:value ""} "all statuses"]
      (for [s ["RUNNING" "COMPLETED" "FAILED" "CANCELLED"]] ^{:key s} [:option {:value s} s])]
     [:input {:type "number" :min 1 :style {:width 80} :value (:limit f) :title "limit"
              :on-change #(do (st/set-filter! :limit (js/parseInt (.. % -target -value)))
                              (act/load-instances!))}]
     ;; --- exact lookup by instance id or correlation (business) key ---
     [:span.spacer]
     [:select {:value (name (:search-by f)) :title "search field"
               :on-change #(do (st/set-filter! :search-by (keyword (.. % -target -value)))
                               (when searching (act/load-instances!)))}
      [:option {:value "correlation"} "correlation id"]
      [:option {:value "id"} "instance id"]]
     [:input {:type "search" :style {:width 220}
              :placeholder (if (= :id (:search-by f)) "instance id…" "correlation id…")
              :value (:search f)
              :on-change #(st/set-filter! :search (.. % -target -value))
              :on-key-down #(when (= (.-key %) "Enter") (act/load-instances!))}]
     [:button.ghost {:on-click act/load-instances! :title "search"} "🔍"]
     (when searching
       [:button.ghost {:title "clear search"
                       :on-click #(do (st/set-filter! :search "") (act/load-instances!))} "✕"])
     [:button.ghost {:on-click act/load-instances! :title "refresh"} "↻"]]))

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
                :on-click #(do (act/load-detail! (:id i)) (st/open-window! :detail))}
           [:td [:code (:id i)]] [:td (:workflow i)]
           [:td [badge (:status i)]] [:td.muted (u/ago (:updatedAt i)) " ago"]])]])))

(defn instances-tab []
  [:div
   [:section.panel
    [:h2 "Instances" [:span.count (count (:instances @db))]]
    [instances-toolbar]
    [instances-table]]
   (when (= :detail (get-in @db [:window :kind]))
     (let [i (get-in @db [:detail :instance])]
       [floating-window {:title [:span "Detail"
                                 (when i [:span.muted {:style {:fontWeight 400}} " · " [:code (:id i)]])]
                         :on-close st/close-window!}
        [detail-body]]))])

;; ---------------------------------------------------------------- workflows tab

(defn workflows-tab []
  (let [{:keys [workflows graph graph-for]} @db]
    [:div
     [:section.panel
      [:h2 "Workflows" [:span.count (count workflows)]]
      (if-not (seq workflows)
        [:div.empty "no workflows registered"]
        [:table [:tbody
                 (for [w workflows]
                   ^{:key w}
                   [:tr {:class (when (= w graph-for) "sel")
                         :on-click #(do (act/load-graph! w) (st/open-window! :diagram))}
                    [:td w]])]])]
     (when (= :diagram (get-in @db [:window :kind]))
       [floating-window {:title [:span "Diagram"
                                 (when graph-for [:span.muted {:style {:fontWeight 400}} " · " graph-for
                                                  " v" (:version graph)])]
                         :on-close st/close-window!}
        (if graph
          [diagram/diagram graph {:selected nil}]
          [:div.empty "select a workflow to see its graph"])])]))

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
          [:td.actions (when (st/can-write?)
                         [:button.danger {:on-click #(act/delete-schedule! (:id s))} "delete"])]])]])])

(defn schedules-tab []
  [:div.cols
   (when (st/can-write?) [schedule-form])
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
        [:td.actions (when (st/can-write?)
                       [:button.primary {:on-click #(swap! open not)} (if @open "close" "deliver")])]]
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

;; ---------------------------------------------------------------- backlog tab

(defn backlog-row [b]
  [:tr {:class (when-not (:covered b) "uncovered")}
   [:td [:strong (:workflow b)]]
   [:td [:code (:version b)]]
   [:td (:queue b)]
   [:td (:readyCount b)]
   [:td.muted (if (pos? (:oldestAvailableAt b)) (u/ago (:oldestAvailableAt b)) "—")]
   [:td (if (:covered b)
          [:span.ok "covered"]
          [:span.bad "no worker"])]])

(defn backlog-tab []
  (let [b (:backlog @db)
        slices (:slices b)
        uncovered (:uncoveredSlices b 0)]
    [:section.panel
     [:h2 "Backlog" [:span.count (count slices)]]
     [:p.muted
      "Work that is READY to dispatch, grouped by what decides who may claim it. A row marked "
      [:strong "no worker"] " is being served by nothing: either no worker polls that queue, or every"
      " worker has scoped itself to other versions. Those tokens sit dispatchable and unclaimed —"
      " the instance still reads RUNNING, so nothing else in this console shows it."]
     (if-not (seq slices)
       [:div.empty "nothing is waiting to be dispatched"]
       [:<>
        (when (pos? uncovered)
          [:div.warn
           (str uncovered " slice" (when (> uncovered 1) "s") " with no worker — "
                (:strandedTasks b) " task(s) stranded")])
        [:table
         [:thead [:tr [:th "workflow"] [:th "version"] [:th "queue"] [:th "ready"]
                  [:th "oldest"] [:th "coverage"]]]
         [:tbody (for [s slices]
                   ^{:key (str (:workflow s) ":" (:version s) ":" (:queue s))}
                   [backlog-row s])]]])]))

;; ---------------------------------------------------------------- performance tab

(defn- ms [n]
  (cond (nil? n) "—"
        (>= n 1000) (str (.toFixed (/ n 1000) 2) " s")
        :else (str (Math/round n) " ms")))

(defn perf-toolbar []
  (let [{:keys [workflow window]} (:perf @db)]
    [:div.toolbar
     [:select {:value workflow
               :on-change #(do (st/set-perf! :workflow (.. % -target -value)) (act/load-perf!))}
      [:option {:value ""} "choose a workflow…"]
      (for [w (:workflows @db)] ^{:key w} [:option {:value w} w])]
     [:select {:value window :title "window"
               :on-change #(do (st/set-perf! :window (.. % -target -value)) (act/load-stats!))}
      (for [[v label] [["15m" "last 15 minutes"] ["1h" "last hour"] ["24h" "last 24 hours"]
                       ["7d" "last 7 days"] ["all" "everything sampled"]]]
        ^{:key v} [:option {:value v} label])]
     [:span.spacer]
     [:button.ghost {:on-click act/load-perf! :title "refresh"} "↻"]]))

(defn stats-panel []
  (let [{:keys [stats graph graph-for perf]} @db
        nodes (:nodes stats)
        top (or (:p95Millis (first nodes)) 0)
        heat (into {} (for [n nodes] [(:nodeId n) (if (pos? top) (/ (:p95Millis n) top) 0)]))
        subs (into {} (for [n nodes] [(:nodeId n) (str "p95 " (ms (:p95Millis n)) " · n=" (:count n))]))
        graph-ok (and graph (= graph-for (:workflow perf)))]
    [:section.panel
     [:h2 "Step durations" [:span.count (count nodes)]]
     [:p.muted
      "How long each step takes where it runs, over the newest timed steps in the window: observed"
      " runs and locally-chained workers report a step's own clock. Slowest p95 first, so the top"
      " row is the bottleneck; the diagram rings each step by its share of that p95."]
     (cond
       (empty? (:workflow perf)) [:div.empty "choose a workflow to see its step durations"]
       (nil? stats) [:div.empty "loading…"]
       (empty? nodes) [:div.empty "no timed steps in this window"]
       :else
       [:<>
        (when graph-ok [diagram/diagram graph {:heat heat :subs subs}])
        [:table
         [:thead [:tr [:th "step"] [:th "runs"] [:th "mean"] [:th "p50"] [:th "p95"] [:th "max"]
                  [:th {:style {:width 180}} "share of slowest p95"]]]
         [:tbody
          (for [n nodes]
            ^{:key (:nodeId n)}
            [:tr {:style {:cursor "default"}}
             [:td [:strong (or (:name n) (:nodeId n))] " " [:code (:nodeId n)]]
             [:td (:count n)]
             [:td.muted (ms (:meanMillis n))]
             [:td (ms (:p50Millis n))]
             [:td {:style {:color (diagram/heat-colour (get heat (:nodeId n)))}} (ms (:p95Millis n))]
             [:td.muted (ms (:maxMillis n))]
             [:td {:style {:width 180 :min-width 180}}
              [:div.bar [:span {:style {:width (str (* 100 (get heat (:nodeId n) 0)) "%")
                                        :background (diagram/heat-colour (get heat (:nodeId n)))}}]]]])]]])]))

(def ^:private anomaly-hint
  {"OUT_OF_ORDER" "a step ran where another was due; the run was resynchronised at the reported step"
   "UNKNOWN_NODE" "a step the graph has no node for; skipped"
   "AFTER_END"    "steps reported after the instance had already ended"
   "INCOMPLETE"   "the run closed before reaching END; the instance was failed"
   "DUPLICATE"    "a step already run ran again outside any loop: at-least-once delivery, most likely; ignored"
   "STALLED"      "no report arrived for longer than the stall threshold; judged as it stood and failed"})

(defn anomalies-panel []
  (let [{:keys [anomalies perf]} @db]
    [:section.panel
     [:h2 "Anomalies" [:span.count (count anomalies)]]
     [:p.muted
      "Where an observed run departed from its declared topology. The server records these instead"
      " of refusing the report, so the rest of the run still yields its timings."]
     (if-not (seq anomalies)
       [:div.empty (if (empty? (:workflow perf))
                     "no anomalies recorded"
                     (str "no anomalies recorded for " (:workflow perf)))]
       [:table
        [:thead [:tr [:th "kind"] [:th "workflow"] [:th "instance"] [:th "expected"] [:th "reported"]
                 [:th "detail"] [:th "when"]]]
        [:tbody
         (for [a anomalies]
           ^{:key (str (:instanceId a) ":" (:at a) ":" (:kind a))}
           [:tr {:title (get anomaly-hint (:kind a))
                 :on-click #(do (act/load-detail! (:instanceId a)) (st/open-window! :detail))}
            [:td [:span.badge.FAILED (:kind a)]]
            [:td (:workflow a) [:span.muted " v" (:version a)]]
            [:td [:code (:instanceId a)]]
            [:td [:code (:expectedNode a)]]
            [:td [:code (:reportedNode a)]]
            [:td.muted {:title (:detail a)} (:detail a)]
            [:td.muted (u/ago (:at a)) " ago"]])]])]))

(defn performance-tab []
  [:div
   [:section.panel
    [:h2 "Performance"]
    [perf-toolbar]]
   [stats-panel]
   [anomalies-panel]
   (when (= :detail (get-in @db [:window :kind]))
     (let [i (get-in @db [:detail :instance])]
       [floating-window {:title [:span "Detail"
                                 (when i [:span.muted {:style {:fontWeight 400}} " · " [:code (:id i)]])]
                         :on-close st/close-window!}
        [detail-body]]))])

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
      :signals   [signals-tab]
      :backlog   [backlog-tab]
      :performance [performance-tab])]
   [toast]])
