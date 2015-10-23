(ns explojure.dataframe.core
  (:require [explojure.dataframe.validation :as valid]
            [explojure.util :as util]
            [clojure.set :as s]
            [clojure.test :as t]))

;; Placeholder to allow compilation.
(declare new-dataframe)

(defprotocol Tabular
  "The Tabular protocol represents tabular data.  (Rows and columns)."

  ($ [this row-spec col-spec]
     [this [df col-spec]]))

(deftype DataFrame
    [colnames    ; a vector of column names, giving column order
     columns     ; a vector of column vectors
     column-ct   ; the number of columns
     colname-idx ; a hash-map of colname => 0-based index
     row-ct      ; the number of rows
     rowname-idx ; (optional) a hash-map of rowname => 0-based index
     ])

(defn new-dataframe
  ([cols vecs]
   ;; ensure data assumptions
   (valid/validate-columns cols vecs)
   
   ;; create a new DataFrame
   (let [column-ct (count cols)
         colname-idx (reduce (fn [m [k i]]
                               (assoc m k i))
                             {}
                             (map vector
                                  cols
                                  (range column-ct)))
         row-ct (count (first vecs))]
     (->DataFrame cols
                  vecs
                  column-ct
                  colname-idx
                  row-ct
                  nil)))


  
  ([cols vecs rownames]
   ;; ensure column assumptions
   (valid/validate-columns cols vecs)
   (let [column-ct (count cols)
         colname-idx (reduce (fn [m [k i]]
                               (assoc m k i))
                             {}
                             (map vector
                                  cols
                                  (range column-ct)))
         row-ct (count (first vecs))]

     ;; ensure rowname assumptions
     (valid/validate-rownames rownames row-ct)
     (let [rowname-idx (reduce (fn [m [k i]]
                                 (assoc m k i))
                               {}
                               (map vector
                                    rownames
                                    (range row-ct)))]
       ;; create a new DataFrame
       (->DataFrame cols
                    vecs
                    column-ct
                    colname-idx
                    row-ct
                    rowname-idx)))))

(defn $df
  "
  Create a new DataFrame. For now, assumes that:
   - all vectors are of same length
  
  Arguments are alternating key value expressions such as one would
  supply to the hash-map or array-map functions.  Keys are column
  names, and values are sequentials (vectors, lists, etc.) containing
  the data for each column.

  Data vector should be the same length.
  "
  [& keyvals]
  (let [idx (util/vrange (count keyvals))
        cols (util/vmap (vec keyvals)
                       (util/vfilter even? idx))
        vecs (util/vmap (vec keyvals)
                      (util/vfilter odd? idx))]
    (new-dataframe cols
                   vecs)))


