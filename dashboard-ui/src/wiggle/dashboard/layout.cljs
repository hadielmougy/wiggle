(ns wiggle.dashboard.layout
  "A small, pure left-to-right layered layout for workflow graphs. Ranks come from a BFS off
   the start node (so the happy path reads left to right); edges that point back to an
   equal-or-earlier rank are the cycles that `doWhile` introduces and are simply drawn as
   curved back-edges. No external graph library — the graphs here are small.")

(def node-w 158)
(def node-h 48)
(def col-gap 210)     ; centre-to-centre horizontal distance between ranks
(def row-gap 74)      ; centre-to-centre vertical distance within a rank
(def margin 40)

(defn- out-edges
  "Outgoing edges of a node as {:to :kind :label}. :kind is :next | :alt | :branch."
  [{:keys [id kind next altNext branches]}]
  (let [pred? (= kind "PREDICATE")]
    (cond-> []
      next    (conj {:from id :to next :kind :next :label (when pred? "yes")})
      altNext (conj {:from id :to altNext :kind :alt :label (if pred? "no" "else")})
      (seq branches) (into (map (fn [b] {:from id :to b :kind :branch :label nil}) branches)))))

(defn all-edges [nodes]
  (mapcat out-edges nodes))

(defn- assign-ranks
  "BFS rank (column index) for each node id, starting at 0 for the start node. Unreachable
   nodes are appended at the max rank + 1 so nothing is dropped."
  [nodes start]
  (let [by-id (into {} (map (juxt :id identity)) nodes)
        succ  (fn [id] (map :to (out-edges (by-id id))))]
    (loop [ranks {start 0}, queue [start]]
      (if-let [id (first queue)]
        (let [r (ranks id)
              nxt (remove ranks (filter by-id (succ id)))]
          (recur (reduce #(assoc %1 %2 (inc r)) ranks nxt)
                 (into (subvec (vec queue) 1) nxt)))
        (let [maxr (reduce max 0 (vals ranks))]
          (reduce (fn [m {:keys [id]}]
                    (if (m id) m (assoc m id (inc maxr))))
                  ranks nodes))))))

(defn layout
  "Returns {:nodes {id -> {:x :y :w :h :node}} :edges [...] :width :height}."
  [{:keys [startNode nodes]}]
  (let [nodes (vec nodes)
        ranks (assign-ranks nodes (or startNode (:id (first nodes))))
        by-rank (->> nodes
                     (sort-by :id)
                     (group-by #(ranks (:id %))))
        placed (into {}
                     (for [[r ns] by-rank
                           [row n] (map-indexed vector (sort-by :id ns))]
                       [(:id n) {:x (+ margin (* r col-gap))
                                 :y (+ margin (* row row-gap))
                                 :w node-w :h node-h :node n}]))
        cols (inc (reduce max 0 (vals ranks)))
        rows (reduce max 1 (map count (vals by-rank)))]
    {:nodes placed
     :edges (all-edges nodes)
     :width  (+ (* cols col-gap) margin)
     :height (+ (* rows row-gap) margin)}))
