(ns explojure.dataframe.construct
  (:require [explojure.dataframe.validate :as valid]
            [explojure.util :as util]))

(deftype DataFrame
    [colnames    ; a vector of column names, giving column order
     columns     ; a vector of column vectors
     column-ct   ; the number of columns
     colname-idx ; a hash-map of colname => 0-based index
     rownames    ; a vector of row names, in order
     row-ct      ; the number of rows
     rowname-idx ; (optional) a hash-map of rowname => 0-based index
     ])

(defn new-dataframe
  ([cols vecs]
   (new-dataframe cols vecs nil))
  
  ([cols vecs rownames]
   ;; ensure column assumptions
   (valid/validate-columns cols vecs)
   (let [column-ct (count cols)
         colname-idx (reduce (fn [m [i k]]
                               (assoc m k i))
                             {}
                             (map-indexed vector cols))
         row-ct (count (first vecs))]
     (let [rowname-idx (if (not (nil? rownames))
                         (do
                           ;; ensure rowname assumptions
                           (valid/validate-rownames rownames row-ct)
                           (let [rowname-idx (reduce (fn [m [k i]]
                                                       (assoc m k i))
                                                     {}
                                                     (map-indexed vector rownames))]
                             rowname-idx))
                         nil)]
       ;; create a new DataFrame
       (->DataFrame cols
                    vecs
                    column-ct
                    colname-idx
                    rownames
                    row-ct
                    rowname-idx)))))

(defn df
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

(defn nil-df
  "Create a dataframe with the specified column names and nrow nils per column."
  [cols nrow]
  (let [ncol (count cols)
        nil-col (util/vrepeat nrow nil)]
    (new-dataframe cols
                   (util/vrepeat ncol
                                 nil-col))))
