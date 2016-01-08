(ns explojure.io
  (:require [explojure.dataframe.core :as dataframe]
            [explojure.dataframe.util :as dfu]
            [explojure.dataframe.impl.get :as raw]
            [explojure.util :as util]
            
            [clojure.java.io :as io])

  (:import [org.apache.commons.csv CSVParser CSVFormat]))

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
    (if (= (subs x 0 1) ":")
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
    (let [header (raw/colnames df)
          rows (raw/row-vectors df)]
      (csv/write-csv writer (concat [header] rows)))))


(defn- record-seq [reader parser i-seq]
  (lazy-seq
   (cons (vec (seq (first i-seq)))
         (if (next i-seq)
           (record-seq reader parser (next i-seq))
           (do
             (.close parser)
             (.close reader)
             nil)))))

(defprotocol Read-CSV
  (read-csv [input]))

(extend-protocol Read-CSV
  String
  (read-csv [s]
    (read-csv (clojure.java.io/reader s)))
  
  Reader
  (read-csv [reader]
    (let [parser (.parse CSVFormat/DEFAULT reader)]
      (record-seq reader parser (seq parser)))))

