(ns explojure.io
  (require [explojure.core :as core]
           [explojure.util :as util]
           
           [clojure.data.csv :as csv]
           [clojure.java.io :as io]))

(defn read-csv [f]
  ;; for now, assuming at least that each CSV file has a header row
  (with-open [reader (io/reader f)]
    (let [rows (doall (csv/read-csv reader))
          header (first rows)
          data (util/t (next rows))]
      (core/->DataFrame (apply array-map (interleave header data))))))

(defn write-csv [df f]
  (with-open [writer (io/writer f)]
    (let [header (core/$colnames tmp)
          rows (util/t (map #(core/$col df %) header))]
      (csv/write-csv writer (concat [header] rows)))))

