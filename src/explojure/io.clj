(ns explojure.io
  (require [explojure.core :as core]
           [explojure.util :as util]
           
           [clojure.data.csv :as csv]
           [clojure.java.io :as io]))

(def empty-string "")
(def null-vals (set [empty-string "na" "nan" "null" "nil"]))

(defn read-nil [x]
  (if (string? x)
    (if (contains? null-vals (clojure.string/lower-case x))
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

(defn read-keyword [x]
  (if (string? x)
    (if (= ":" (subs x 0 1))
      (keyword (subs x 1))
      x)
    x))

(def types-xf (comp (map clojure.string/trim)
                    (map read-nil)
                    (map read-long)
                    (map read-double)
                    (map read-keyword)))

(defn write-csv [df f]
  (with-open [writer (io/writer f)]
    (let [header (core/$colnames df)
          rows (core/$rows df)]
      (csv/write-csv writer (concat [header] rows)))))



(defn read-csv [f]
  ;; for now, assuming at least that each CSV file has a header row
  (with-open [reader (io/reader f)]
    (let [csv-seq (csv/read-csv reader)
          headers (first csv-seq)
          rows (rest csv-seq)
          col-ct (count headers)
          cols (vec (repeatedly col-ct #(vector)))]
      (loop [cols cols
           rows rows]
        (if (seq rows)
          (recur (loop [cols cols
                        i 0
                        row-vals (sequence types-xf (first rows))]
                   (if (seq row-vals)
                     (let [col-vals (get cols i)]
                       (recur (assoc cols i (conj col-vals (first row-vals)))
                              (inc i)
                              (rest row-vals)))
                     cols))
                 (rest rows))
          (core/->DataFrame headers
                            (apply hash-map (interleave headers cols))))))))



