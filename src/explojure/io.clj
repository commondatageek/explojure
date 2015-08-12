(ns explojure.io
  (require [explojure.core :as core]
           [explojure.util :as util]
           
           [clojure.data.csv :as csv]
           [clojure.java.io :as io]))

(def empty-string "")

(defn read-nil [x]
  (if (string? x)
    (if (= x empty-string)
      nil
      x)
    x))

(defn read-long [x]
  (if (string? x)
    (try
      (Long/parseLong x)
      (catch Exception e
        x))
    x))

(defn read-double [x]
  (if (string? x)
    (try
      (Double/parseDouble x)
      (catch Exception e
        x))
    x))

(def types-xf (comp (map read-nil)
                    (map read-long)
                    (map read-double)))

(defn read-csv [f]
  ;; for now, assuming at least that each CSV file has a header row
  (with-open [reader (io/reader f)]
    (let [rows (doall (csv/read-csv reader))
          header (first rows)
          data (util/t (map #(sequence types-xf %) (next rows)))]
      (core/->DataFrame header
                        (apply hash-map (interleave header data))))))

(defn write-csv [df f]
  (with-open [writer (io/writer f)]
    (let [header (core/$colnames df)
          rows (util/t (map #(core/$col df %) header))]
      (csv/write-csv writer (concat [header] rows)))))
