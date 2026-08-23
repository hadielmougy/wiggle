(ns wiggle.dashboard.util)

(defn ago [ms]
  (if (or (nil? ms) (zero? ms))
    ""
    (let [s (max 0 (/ (- (js/Date.now) ms) 1000))]
      (cond (< s 60)   (str (int s) "s")
            (< s 3600) (str (int (/ s 60)) "m")
            (< s 86400)(str (int (/ s 3600)) "h")
            :else      (str (int (/ s 86400)) "d")))))

(defn in-secs [ms]
  (when (and ms (pos? ms))
    (let [d (Math/round (/ (- ms (js/Date.now)) 1000))]
      (if (neg? d) "overdue" (str "in " d "s")))))

(defn ts [ms]
  (when (and ms (pos? ms)) (.toLocaleString (js/Date. ms))))

(defn every-str [ms]
  (cond (zero? ms) ""
        (zero? (mod ms 3600000)) (str "every " (/ ms 3600000) "h")
        (zero? (mod ms 60000))   (str "every " (/ ms 60000) "m")
        (zero? (mod ms 1000))    (str "every " (/ ms 1000) "s")
        :else                    (str "every " ms "ms")))

(defn pretty-json [x]
  (.stringify js/JSON (clj->js x) nil 2))

(defn parse-json-or [s fallback]
  (try (js->clj (.parse js/JSON s) :keywordize-keys false)
       (catch :default _ fallback)))
