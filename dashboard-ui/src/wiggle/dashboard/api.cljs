(ns wiggle.dashboard.api
  "Thin wrappers over the dashboard's JSON control-plane endpoints. Every call returns a
   promise resolving to parsed EDN-ish data (keywordised keys) or rejecting with an
   {:status :message} map so callers can surface a clean error."
  (:require [clojure.string :as str]))

(defn- parse-json [s]
  (js->clj (.parse js/JSON s) :keywordize-keys true))

(defn- request [method path body]
  (let [opts (cond-> #js {:method method :headers #js {"Content-Type" "application/json"}}
               body (doto (aset "body" (.stringify js/JSON (clj->js body)))))]
    (-> (js/fetch path opts)
        (.then (fn [resp]
                 (.then (.text resp)
                        (fn [text]
                          (let [data (when (seq text) (parse-json text))]
                            (if (.-ok resp)
                              data
                              (throw (ex-info (or (:error data) (str "HTTP " (.-status resp)))
                                              {:status (.-status resp)})))))))))))

(defn GET    [path]      (request "GET" path nil))
(defn POST   [path body] (request "POST" path body))
(defn DELETE [path]      (request "DELETE" path nil))

(defn- enc [s] (js/encodeURIComponent s))

;; ---- specific endpoints ------------------------------------------------------

(defn auth          [] (GET "/api/auth"))
(defn cluster       [] (GET "/api/cluster"))
(defn workflows     [] (GET "/api/workflows"))
(defn workflow-graph [name] (GET (str "/api/workflows/" (enc name))))
(defn signals       [] (GET "/api/signals"))
(defn schedules     [] (GET "/api/schedules"))

(defn instances [{:keys [workflow status limit search search-by]}]
  ;; A non-empty search is an exact lookup by instance id or correlation key and takes over the query
  ;; (workflow/status don't apply); otherwise it's the normal filtered listing.
  (let [searching (seq (some-> search str/trim))
        qs (cond-> []
             searching            (conj (str (name (or search-by :correlation)) "=" (enc (str/trim search))))
             (not searching)      (into (cond-> []
                                          (seq workflow) (conj (str "workflow=" (enc workflow)))
                                          (seq status)   (conj (str "status=" (enc status)))))
             limit                (conj (str "limit=" limit)))]
    (GET (str "/api/instances" (when (seq qs) (str "?" (str/join "&" qs)))))))

(defn instance [id] (GET (str "/api/instances/" (enc id))))

(defn cancel-instance [id reason]
  (POST (str "/api/instances/" (enc id) "/cancel"
             (when (seq reason) (str "?reason=" (enc reason)))) nil))

(defn deliver-signal [instance-id signal payload]
  (POST (str "/api/instances/" (enc instance-id) "/signal/" (enc signal)) payload))

(defn create-schedule [body] (POST "/api/schedules" body))
(defn delete-schedule [id]   (DELETE (str "/api/schedules/" (enc id))))
