(ns wiggle.dashboard.views
  (:require [reagent.core :as r]
            [clojure.string :as str]
            [wiggle.dashboard.state :as st :refer [db]]
            [wiggle.dashboard.actions :as act]
            [wiggle.dashboard.util :as u]))

;; ---------------------------------------------------------------- shared bits

(defn badge [status]
  [:span {:class (str "badge " status)} status])

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
      (for [[k label] (cond-> [[:instances "Instances"] [:workflows "Workflows"]
                               [:schedules "Schedules"] [:signals "Signals"]
                               [:backlog "Backlog"] [:performance "Performance"]]
                        (st/can-manage-users?) (conj [:users "Users"]))]
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
        (when (st/can-change-password?)
          [:button.ghost {:on-click #(st/open-window! :password)} "Password"])
        [:button.ghost {:on-click #(set! (.. js/window -location -href) "/logout")} "Log out"]])]))

;; ---------------------------------------------------------------- signal form

(defn password-field
  "One password box and a button. on-submit is (fn [password]); the box clears after it fires."
  [placeholder on-submit label]
  (let [v (r/atom "")]
    (fn [placeholder on-submit label]
      [:div.row {:style {:padding "10px 14px"}}
       [:input {:type "password" :style {:flex 1} :value @v :placeholder placeholder
                :auto-complete "new-password"
                :on-change #(reset! v (.. % -target -value))}]
       [:button.primary {:on-click #(let [p @v] (reset! v "") (on-submit p))} label]])))

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

(defn- ms [n]
  (cond (nil? n) "—"
        (>= n 60000) (str (.toFixed (/ n 60000) 1) " min")
        (>= n 1000) (str (.toFixed (/ n 1000) 2) " s")
        :else (str (Math/round n) " ms")))

(defn- clock [t]
  (when (and t (pos? t)) (.toLocaleTimeString (js/Date. t) [] #js {:hour12 false})))

(defn- step-duration
  "How long the step ran: its own clock when settled, the time so far while it is running."
  [{:keys [startedAt finishedAt status]}]
  (cond (and startedAt finishedAt) (- finishedAt startedAt)
        (and startedAt (= status "RUNNING")) (- (js/Date.now) startedAt)))

(defn- queue-wait
  "How long the step sat ready before a worker took it; nil where that is not known."
  [{:keys [startedAt availableAt]}]
  (when (and startedAt (pos? availableAt) (>= startedAt availableAt))
    (- startedAt availableAt)))

(defn- step-label [names t]
  (or (get names (:nodeId t))
      (not-empty (:activity t))
      (:nodeId t)))

(defn- truncated? [v] (and (map? v) (contains? v :$truncated)))

(defn- json-block
  "A recorded input/output: pretty JSON, or the head of one the server truncated."
  [v]
  (if (truncated? v)
    [:div
     [:div.muted {:style {:margin-bottom 6}}
      "truncated by the server: " (:$truncated v) " characters, first " (count (:head v)) " shown"]
     [:pre (:head v)]]
    [:pre (u/pretty-json v)]))

(defn- step-output [t]
  (cond
    (= (:kind t) "PREDICATE")
    (if (nil? (:output t)) [:div.muted "—"] [:div "branch " [:strong (if (:output t) "yes" "no")]])
    (some? (:output t)) [json-block (:output t)]
    (and (= (:status t) "DONE") (some? (:input t))) [:div.muted "returned nothing: the context was left unchanged"]
    (some? (:input t)) [:div.muted "no output: the step has not completed"]
    :else [:div.muted "not recorded"]))

(defn- step-detail [t]
  [:div.step-detail
   [:div.step-io
    [:div [:h3 "Input"] (if (some? (:input t)) [json-block (:input t)] [:div.muted "not recorded"])]
    [:div [:h3 "Output"] [step-output t]]]
   [:dl.facts
    [:dt "retries"] [:dd (or (:attempt t) 0)]
    [:dt "status"] [:dd [badge (:status t)]]
    [:dt "ready at"] [:dd (or (u/ts (:availableAt t)) "—")]
    [:dt "started"] [:dd (or (u/ts (:startedAt t)) "—")]
    [:dt "finished"] [:dd (or (u/ts (:finishedAt t)) "—")]
    [:dt "duration"] [:dd (ms (step-duration t))]
    [:dt "waited in queue"] [:dd (ms (queue-wait t))]
    (when (:leaseOwner t) [:<> [:dt "leased to"] [:dd (:leaseOwner t)]])
    [:dt "token"] [:dd [:code (:id t)]]]
   (when (:lastError t)
     [:div [:h3 "Last error"] [:pre.err (:lastError t)]])])

(def ^:private step-cols 8)

(defn steps-table
  "Every token of the instance in the order it was created, one row per step run. Clicking a row
   expands it in place to the step's input, output, retries and timing."
  []
  (let [open (r/atom #{})]
    (fn [tokens names]
      (let [rows (sort-by (juxt :createdAt :id) tokens)]
        [:table.steps
         [:thead [:tr [:th {:style {:width "6%"}} "#"] [:th {:style {:width "28%"}} "step"]
                  [:th {:style {:width "12%"}} "kind"] [:th {:style {:width "12%"}} "status"]
                  [:th {:style {:width "10%"}} "retries"] [:th {:style {:width "11%"}} "started"]
                  [:th {:style {:width "11%"}} "duration"] [:th {:style {:width "10%"}} "waited"]]]
         [:tbody
          (for [[n t] (map-indexed vector rows)
                :let [open? (contains? @open (:id t))]]
            ^{:key (:id t)}
            [:<>
             [:tr {:class (when open? "sel")
                   :on-click #(swap! open (fn [o] (if (contains? o (:id t)) (disj o (:id t)) (conj o (:id t)))))}
              [:td.muted (if open? "▾ " "▸ ") (inc n)]
              [:td [:strong (step-label names t)] " " [:code.muted (:nodeId t)]]
              [:td.muted (:kind t)]
              [:td [badge (:status t)]]
              [:td {:class (when (pos? (:attempt t)) "retried")} (or (:attempt t) 0)]
              [:td.muted (or (clock (:startedAt t)) "—")]
              [:td (ms (step-duration t))]
              [:td.muted (ms (queue-wait t))]]
             (when open?
               [:tr.expanded [:td {:col-span step-cols} [step-detail t]]])])]]))))

(defn detail-body []
  (let [{:keys [detail selected graph graph-for]} @db
        i (:instance detail)]
    (cond
       (not selected) [:div.empty "select an instance to see its steps"]
       (not i)        [:div.empty "loading…"]
       :else
       (let [tokens (:tokens detail)
             names (when (= graph-for (:workflow i))
                     (into {} (for [n (:nodes graph)] [(:id n) (:name n)])))
             ran (filter :startedAt tokens)
             first-start (some->> (seq ran) (map :startedAt) (apply min))
             last-finish (some->> (seq (keep :finishedAt tokens)) (apply max))]
         [:div
          [:div.toolbar
           [badge (:status i)]
           [:span.muted (:workflow i) " v" (:version i)]
           [:div.spacer {:style {:margin-left "auto"}}]
           (when (and (st/can-write?) (= (:status i) "RUNNING"))
             [:button.danger {:on-click #(act/cancel! (:id i) "cancelled from dashboard")} "cancel"])]

          [:dl.facts.summary
           [:dt "started"] [:dd (or (u/ts (:createdAt i)) "—")]
           [:dt "updated"] [:dd (u/ago (:updatedAt i)) " ago"]
           [:dt "steps run"] [:dd (count ran)]
           [:dt "retries"] [:dd (reduce + 0 (keep :attempt tokens))]
           [:dt "elapsed"] [:dd (ms (when (and first-start last-finish (>= last-finish first-start))
                                      (- last-finish first-start)))]]

          (when (:error i) [:pre.err {:style {:margin "0 14px 10px"}} (:error i)])

          ;; any signal this instance is waiting on -> inline deliver
          (for [t tokens
                :when (and (= (:kind t) "SIGNAL") (= (:status t) "AWAITING"))]
            ^{:key (:id t)}
            [:div {:style {:borderTop "1px solid var(--line)"}}
             [:div {:style {:padding "10px 14px 0" :color "var(--warn)"}}
              "waiting for signal " [:strong (:activity t)]]
             (when (st/can-write?)
               [signal-form (:activity t) #(act/signal! (:id i) (:activity t) %)])])

          [:h2.sub "Steps"]
          (if (seq tokens)
            ^{:key (:id i)} [steps-table tokens names]
            [:div.empty "no steps yet"])

          [:h2.sub "Current context"]
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

(defn- successors [n]
  (remove nil? (concat [(:next n) (:altNext n)] (:branches n))))

(defn- in-flow-order
  "The graph's nodes breadth-first from its start, so a workflow reads in the order it runs; any node
   the walk does not reach comes last."
  [{:keys [nodes startNode]}]
  (let [by-id (into {} (map (juxt :id identity)) nodes)]
    (loop [queue (if startNode [startNode] []), seen #{}, out []]
      (if-let [id (first queue)]
        (if (or (seen id) (not (by-id id)))
          (recur (subvec queue 1) seen out)
          (recur (into (subvec queue 1) (successors (by-id id))) (conj seen id) (conj out (by-id id))))
        (into out (remove #(seen (:id %)) nodes))))))

(defn- retry-text [{:keys [maxAttempts]}]
  (cond (nil? maxAttempts) "—"
        (>= maxAttempts 10000) "until it succeeds"
        :else (str "up to " maxAttempts " attempts")))

(defn- workflow-steps [graph]
  [:table.steps
   [:thead [:tr [:th "step"] [:th "kind"] [:th "queue"] [:th "next"] [:th "retry"]]]
   [:tbody
    (for [n (in-flow-order graph)]
      ^{:key (:id n)}
      [:tr {:style {:cursor "default"}}
       [:td [:strong (:name n)] " " [:code.muted (:id n)]]
       [:td.muted (:kind n)]
       [:td.muted (or (:queue n) "—")]
       [:td.muted (str/join ", " (successors n))]
       [:td.muted (retry-text (:retry n))]])]])

(defn workflows-tab []
  (let [{:keys [workflows graph graph-for wf-open]} @db]
    [:section.panel
     [:h2 "Workflows" [:span.count (count workflows)]]
     (if-not (seq workflows)
       [:div.empty "no workflows registered"]
       [:table [:tbody
                (for [w workflows
                      :let [open? (= w wf-open)
                            loaded (when (= w graph-for) graph)]]
                  ^{:key w}
                  [:<>
                   [:tr {:class (when open? "sel")
                         :on-click #(if open?
                                      (swap! db assoc :wf-open nil)
                                      (do (swap! db assoc :wf-open w) (act/load-graph! w)))}
                    [:td (if open? "▾ " "▸ ") w
                     (when (and open? loaded) [:span.muted " v" (:version loaded)])]]
                   (when open?
                     [:tr.expanded [:td
                                    (if loaded [workflow-steps loaded] [:div.empty "loading…"])]])])]])]))

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

(defn- heat-colour
  "green -> amber -> red for a 0..1 share of the slowest step's p95."
  [h]
  (cond (nil? h) nil
        (< h 0.34) "#3ecf7a"
        (< h 0.67) "#c9a86a"
        :else "#ff8080"))

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
  (let [{:keys [stats perf]} @db
        nodes (:nodes stats)
        top (or (:p95Millis (first nodes)) 0)
        heat (into {} (for [n nodes] [(:nodeId n) (if (pos? top) (/ (:p95Millis n) top) 0)]))]
    [:section.panel
     [:h2 "Step durations" [:span.count (count nodes)]]
     [:p.muted
      "How long each step takes by the handler's own clock, over the newest timed steps in the"
      " window, with how long it waited to be claimed, so a slow step and a starved one read"
      " differently. Slowest p95 first, so the top row is the bottleneck."]
     (cond
       (empty? (:workflow perf)) [:div.empty "choose a workflow to see its step durations"]
       (nil? stats) [:div.empty "loading…"]
       (empty? nodes) [:div.empty "no timed steps in this window"]
       :else
       [:<>
        [:table
         [:thead [:tr [:th "step"] [:th "runs"] [:th "mean"] [:th "p50"] [:th "p95"] [:th "max"]
                  [:th {:title "time from ready to claimed by a worker"} "wait p50 / p95"]
                  [:th {:style {:width 180}} "share of slowest p95"]]]
         [:tbody
          (for [n nodes]
            ^{:key (:nodeId n)}
            [:tr {:style {:cursor "default"}}
             [:td [:strong (or (:name n) (:nodeId n))] " " [:code (:nodeId n)]]
             [:td (:count n)]
             [:td.muted (ms (:meanMillis n))]
             [:td (ms (:p50Millis n))]
             [:td {:style {:color (heat-colour (get heat (:nodeId n)))}} (ms (:p95Millis n))]
             [:td.muted (ms (:maxMillis n))]
             [:td.muted (if (pos? (or (:waitP95Millis n) 0))
                          (str (ms (:waitP50Millis n)) " / " (ms (:waitP95Millis n)))
                          "—")]
             [:td {:style {:width 180 :min-width 180}}
              [:div.bar [:span {:style {:width (str (* 100 (get heat (:nodeId n) 0)) "%")
                                        :background (heat-colour (get heat (:nodeId n)))}}]]]])]]])]))

(defn performance-tab []
  [:div
   [:section.panel
    [:h2 "Performance"]
    [perf-toolbar]]
   [stats-panel]
   (when (= :detail (get-in @db [:window :kind]))
     (let [i (get-in @db [:detail :instance])]
       [floating-window {:title [:span "Detail"
                                 (when i [:span.muted {:style {:fontWeight 400}} " · " [:code (:id i)]])]
                         :on-close st/close-window!}
        [detail-body]]))])

;; ---------------------------------------------------------------- users tab

(defn user-form []
  (let [s (r/atom {:user "" :password "" :role "viewer"})]
    (fn []
      (let [{:keys [user password role]} @s]
        [:section.panel
         [:h2 "New user"]
         [:div {:style {:padding 14}}
          [:div.field [:span "name"]
           [:input {:value user :placeholder "letters, digits, dot, dash, underscore"
                    :on-change #(swap! s assoc :user (.. % -target -value))}]]
          [:div.field [:span "password"]
           [:input {:type "password" :value password :placeholder "at least 8 characters"
                    :auto-complete "new-password"
                    :on-change #(swap! s assoc :password (.. % -target -value))}]]
          [:div.field [:span "role"]
           [:select {:value role :on-change #(swap! s assoc :role (.. % -target -value))}
            [:option {:value "viewer"} "viewer — read-only"]
            [:option {:value "admin"} "admin — full access, manages users"]]]
          [:div.row
           [:button.primary
            {:on-click #(do (act/create-user! {:user user :password password :role role})
                            (reset! s {:user "" :password "" :role "viewer"}))}
            "create user"]]
          [:p.muted {:style {:margin "10px 0 0"}}
           "The account is stored on this console, hashed. Tell the person their password out of band;"
           " they can change it from here once they sign in."]]]))))

(defn users-list []
  (let [resetting (r/atom nil)]
    (fn []
      [:section.panel
       [:h2 "Users" [:span.count (count (:users @db))]]
       [:p.muted
        "Who can sign in to this console. A built-in account comes from the environment where the"
        " console runs, so its password and role are set there, not here."]
       (if-not (seq (:users @db))
         [:div.empty "no accounts yet"]
         [:table
          [:thead [:tr [:th "user"] [:th "role"] [:th "source"] [:th "created"] [:th ""]]]
          [:tbody
           (for [u (:users @db)]
             ^{:key (:name u)}
             [:<>
              [:tr
               [:td [:strong (:name u)]]
               [:td [:span.badge {:class (when (= (:role u) "admin") "RUNNING")} (:role u)]]
               [:td.muted (if (:builtin u) "environment" "this console")]
               [:td.muted (if (:builtin u) "—" (str (u/ago (:createdAt u)) " ago"))]
               [:td.actions
                (when-not (:builtin u)
                  [:<>
                   [:button.ghost {:on-click #(swap! resetting (fn [c] (when-not (= c (:name u)) (:name u))))}
                    (if (= @resetting (:name u)) "close" "set password")]
                   [:button.danger {:on-click #(act/delete-user! (:name u))} "delete"]])]]
              (when (= @resetting (:name u))
                [:tr [:td {:col-span 5 :style {:overflow "visible" :max-width "none"}}
                      [password-field "new password"
                       (fn [p] (act/reset-password! (:name u) p) (reset! resetting nil))
                       "set password"]]])])]])])))

(defn users-tab []
  ;; The form is narrow and the table is not: give the table the full width rather than half of it.
  [:div.cols.wide-left
   [user-form]
   [users-list]])

;; ---------------------------------------------------------------- root

(defn toast []
  (when-let [t (:toast @db)]
    [:div {:class (str "toast " (name (:kind t)))} (:text t)]))

(defn change-password-window []
  (let [s (r/atom {:current "" :next "" :confirm ""})]
    (fn []
      (let [{:keys [current next confirm]} @s]
        [floating-window {:title [:span "Change password"
                                  [:span.muted {:style {:fontWeight 400}} " · " (get-in @db [:auth :user])]]
                          :on-close st/close-window!}
         [:div {:style {:padding 14}}
          [:div.field [:span "current password"]
           [:input {:type "password" :value current :auto-complete "current-password"
                    :on-change #(swap! s assoc :current (.. % -target -value))}]]
          [:div.field [:span "new password"]
           [:input {:type "password" :value next :placeholder "at least 8 characters"
                    :auto-complete "new-password"
                    :on-change #(swap! s assoc :next (.. % -target -value))}]]
          [:div.field [:span "new password again"]
           [:input {:type "password" :value confirm :auto-complete "new-password"
                    :on-change #(swap! s assoc :confirm (.. % -target -value))}]]
          [:div.row
           [:button.primary
            {:on-click #(if (not= next confirm)
                          (st/toast! :err "the two new passwords do not match")
                          (act/change-password! current next st/close-window!))}
            "change password"]]
          [:p.muted {:style {:margin "10px 0 0"}}
           "Your other sessions are signed out; this one stays."]]]))))

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
      :performance [performance-tab]
      :users     [users-tab])]
   (when (= :password (get-in @db [:window :kind])) [change-password-window])
   [toast]])
