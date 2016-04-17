(ns explojure.io
  (:require [explojure.dataframe.core :as dataframe]
            [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.util :as dfu]
            [explojure.dataframe.impl.get :as raw]
            [explojure.util :as util]
            
            [clojure.java.io :as io])

  (:import [org.apache.commons.csv CSVParser CSVFormat CSVPrinter]))

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
  (with-open [writer (io/writer f)
              printer (.print CSVFormat/DEFAULT writer)]
    (let [records (concat [(raw/colnames df)]
                          (raw/row-vectors df))]
      (doseq [r records]
        (.printRecord printer (into-array Object r))))))


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
  
  java.io.Reader
  (read-csv [reader & {:keys [header] :or [header true]}]
    (let [parser (.parse CSVFormat/DEFAULT reader)
          records (map #(sequence types-xf %)
                       (record-seq reader parser (seq parser)))
          header-vals (if header
                        (vec (first records))
                        (vec (range (count (first records)))))
          rows (if header
                 (next records)
                 records)]
      (ctor/new-dataframe header-vals
                          (if (= (count rows) 0)
                            (util/vrepeat (count header-vals) [])
                            (util/rows->cols rows))))))
