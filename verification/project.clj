(defproject wiggle-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen test harness for the Wiggle workflow engine + cell coordinator."
  :url "https://github.com/hadielmougy/wiggle"
  :license {:name "Apache-2.0"}
  :main wiggle-jepsen.core
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/tools.logging "1.3.0"]
                 ;; Jepsen bundles Elle (elle.rw-register / elle.list-append).
                 [jepsen "0.3.7"]
                 ;; The Wiggle Java client + its transitive deps (wiggle-core, wiggle-proto, grpc,
                 ;; protobuf). Resolve from ~/.m2 after publishing locally (see README):
                 ;;   ./gradlew :core:publishToMavenLocal :proto:publishToMavenLocal \
                 ;;             :client:publishToMavenLocal -x signMavenPublication
                 [io.github.hadielmougy/wiggle-client "2.1.5"]]
  :jvm-opts ["-Djava.awt.headless=true" "-server"]
  :repl-options {:init-ns wiggle-jepsen.core})
