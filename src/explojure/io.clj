(ns explojure.io
  (require [explojure.util :as util]
           [clojure.data.csv :as csv]
           [explojure.core :as core]))

(defn read-csv [f]
  ;; for now, assuming at least that each CSV file has a header row
  (let [rows (csv/read-csv (clojure.java.io/reader f))
        header (first rows)
        data (util/t (next rows))]
    (core/->DataFrame (apply array-map (interleave header data)))))





