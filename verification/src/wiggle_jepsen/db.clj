(ns wiggle-jepsen.db
  "The cluster lifecycle. This scaffold runs in EXTERNAL mode: it assumes a Wiggle cluster (and,
   in coordinator mode, a coordinator) is already running -- e.g. started by
   scripts/coordinator-integration.sh or a docker-compose. Jepsen only drives load and faults.

   To let Jepsen own the lifecycle (install, start, kill, pause) implement setup!/teardown! and a
   node kill/pause against your substrate -- typically via jepsen.control over SSH to the nodes
   named in --nodes. That also unlocks a real kill/pause nemesis (jepsen.nemesis/node-start-stopper)."
  (:require [clojure.tools.logging :refer [info]]
            [jepsen.db :as db]))

(defn external-cluster []
  (reify db/DB
    (setup!    [_ _test node] (info "external cluster: assuming" node "is already up"))
    (teardown! [_ _test node] (info "external cluster: leaving" node "running"))))
