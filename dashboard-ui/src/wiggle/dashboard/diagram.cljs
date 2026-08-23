(ns wiggle.dashboard.diagram
  "Renders a laid-out workflow graph as SVG. When given a token-status map it overlays an
   instance trace: each node is ringed by the status of its token (done / running / failed /
   waiting), turning the static graph into a live picture of where an instance is."
  (:require [wiggle.dashboard.layout :as layout]))

(def kind-fill
  {"TASK"         "#1b2740"
   "PREDICATE"    "#2a1f40"
   "SLEEP"        "#242832"
   "FORK"         "#123230"
   "DYN_FORK"     "#123230"
   "JOIN"         "#123230"
   "SIGNAL"       "#2f2718"
   "SUB_WORKFLOW" "#1d1b3a"
   "END"          "#14251b"})

(def kind-stroke
  {"TASK" "#2d4a80" "PREDICATE" "#4a2d80" "SLEEP" "#3a4152"
   "FORK" "#1f6a63" "DYN_FORK" "#1f6a63" "JOIN" "#1f6a63"
   "SIGNAL" "#7a6120" "SUB_WORKFLOW" "#3a3580" "END" "#1f5a3a"})

;; Colour a node's trace ring by the status of its token.
(def status-ring
  {"DONE" "#3ecf7a" "JOINED" "#3ecf7a" "COMPLETED" "#3ecf7a"
   "RUNNING" "#5b9dff" "READY" "#5b9dff"
   "FAILED" "#ff8080"
   "WAITING" "#c9a86a" "AWAITING" "#c9a86a"
   "CANCELLED" "#c9a86a"})

(defn- kind-glyph [kind]
  (get {"TASK" "▸" "PREDICATE" "?" "SLEEP" "⏱" "FORK" "⋔" "DYN_FORK" "⋔"
        "JOIN" "⋈" "SIGNAL" "✋" "SUB_WORKFLOW" "▣" "END" "■"} kind "•"))

(defn- edge-path
  "Cubic-bezier path string between two placed nodes. Forward edges bow horizontally; back
   edges (target at or left of source) loop out to the left."
  [from to]
  (let [sx (+ (:x from) (:w from)), sy (+ (:y from) (/ (:h from) 2))
        tx (:x to),               ty (+ (:y to) (/ (:h to) 2))
        back? (<= tx sx)]
    (if back?
      (let [tx2 (:x to) ty2 (+ (:y to) (/ (:h to) 2))
            dx 40 dy 34]
        (str "M" sx " " sy
             " C" (+ sx dx) " " (- sy dy) " " (- tx2 dx) " " (- ty2 dy) " " tx2 " " ty2))
      (let [mx (/ (+ sx tx) 2)]
        (str "M" sx " " sy " C" mx " " sy " " mx " " ty " " tx " " ty)))))

(defn- edge-mid [from to]
  (let [sx (+ (:x from) (:w from)), sy (+ (:y from) (/ (:h from) 2))
        tx (:x to),               ty (+ (:y to) (/ (:h to) 2))]
    [(/ (+ sx tx) 2) (- (/ (+ sy ty) 2) 5)]))

(defn- node-box [{:keys [x y w h node]} status on-click selected?]
  (let [kind (:kind node)
        ring (status-ring status)]
    [:g.node-box {:transform (str "translate(" x "," y ")")
                  :on-click #(when on-click (on-click (:id node)))
                  :style {:cursor (when on-click "pointer")}}
     (when ring
       [:rect {:x -3 :y -3 :width (+ w 6) :height (+ h 6) :rx 11
               :fill "none" :stroke ring :stroke-width 2.5 :opacity 0.9}])
     [:rect {:width w :height h :rx 8
             :fill (get kind-fill kind "#1b2130")
             :stroke (if selected? "#5b9dff" (get kind-stroke kind "#2a2f3a"))
             :stroke-width (if selected? 2 1.5)}]
     [:text.node-label {:x 12 :y 20} (str (kind-glyph kind) "  " (:name node))]
     [:text.node-sub {:x 12 :y 37}
      (str kind
           (when (:queue node) (str " · " (:queue node)))
           (when (:activity node) (str " · " (:activity node))))]]))

(defn diagram
  "graph: the /api/workflows/{name} payload. statuses: optional {node-id -> token-status}.
   on-node: optional (fn [node-id]) click handler."
  [graph {:keys [statuses on-node selected]}]
  (let [{:keys [nodes edges width height]} (layout/layout graph)]
    [:div.diagram-wrap
     [:svg {:width (max width 320) :height (max height 140)
            :viewBox (str "0 0 " (max width 320) " " (max height 140))}
      [:defs
       [:marker {:id "arrow" :viewBox "0 0 10 10" :refX 9 :refY 5
                 :markerWidth 7 :markerHeight 7 :orient "auto-start-reverse"}
        [:path {:d "M0,0 L10,5 L0,10 z" :fill "#5a6272"}]]]
      ;; edges first, under the nodes
      (into [:g]
            (for [{:keys [from to kind label]} edges
                  :let [f (get nodes from), t (get nodes to)]
                  :when (and f t)]
              ^{:key (str from "->" to "-" (name kind))}
              [:g
               [:path {:class (str "edge " (name kind))
                       :d (edge-path f t) :marker-end "url(#arrow)"}]
               (when label
                 (let [[mx my] (edge-mid f t)]
                   [:text.edge-label {:x mx :y my :text-anchor "middle"} label]))]))
      ;; nodes on top
      (into [:g]
            (for [[id p] nodes]
              ^{:key id}
              [node-box p (get statuses id) on-node (= id selected)]))]
     [:div.legend
      (for [[k _] (sort kind-fill)]
        ^{:key k}
        [:span {:style {:margin-right 4}}
         [:span.sw {:style {:background (get kind-fill k) :border-color (get kind-stroke k)}}]
         k])]]))
