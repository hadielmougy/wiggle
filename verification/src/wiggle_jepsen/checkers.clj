(ns wiggle-jepsen.checkers
  "Checkers composed over the recorded history:

     elle-register  -- transactional register anomalies (lost/stale writes, cycles) via Elle
     single-cell    -- a given instance is only ever served by one cell (T12 invariant)
     liveness       -- after faults heal, every key is readable again (the system recovered)

   Elle is the transactional lens; single-cell and liveness are Wiggle-specific invariants
   Elle's cycle model does not express."
  (:require [clojure.set :as set]
            [jepsen.checker :as checker]
            [elle.rw-register :as rw]))

(defn elle-register
  "Elle's read/write register checker. Real-time order (Jepsen supplies it) lets us ask for
   strict serializability; drop to :serializable if you record only process order."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (rw/check {:consistency-models [:strict-serializable]
                 :directory (str (:directory opts) "/elle")}
                history))))

(defn- op-key [op] (-> op :value first second))

(defn single-cell
  "Every OK op that recorded a :cell for a key must have used the same cell -- no instance is
   ever served by two cells (T12). Ops without a :cell (direct mode) are ignored."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [by-key (->> history
                        (filter #(= :ok (:type %)))
                        (filter :cell)
                        (group-by op-key))
            violations (for [[k ops] by-key
                             :let [cells (into (sorted-set) (map :cell ops))]
                             :when (> (count cells) 1)]
                         {:key k :cells cells})]
        {:valid?     (empty? violations)
         :violations (vec violations)}))))

(defn liveness
  "The cluster recovered: every key has a successful final read (the reads emitted in the
   post-fault quiescent phase, tagged :final?)."
  []
  (reify checker/Checker
    (check [_ test history opts]
      (let [wanted (set (:keyspace test))
            read   (->> history
                        (filter :final?)
                        (filter #(= :ok (:type %)))
                        (map op-key)
                        set)
            missing (set/difference wanted read)]
        {:valid?         (empty? missing)
         :keys-total     (count wanted)
         :keys-read-back (count read)
         :missing        (vec missing)}))))
