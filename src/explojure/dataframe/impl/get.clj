(ns explojure.dataframe.impl.get
  (:require [explojure.dataframe.construct]
            [explojure.util :as util]))

(defn colnames [^explojure.dataframe.construct.DataFrame this]
  (.colnames this))

(defn rownames [^explojure.dataframe.construct.DataFrame this]
  (.rownames this))

(defn ncol [^explojure.dataframe.construct.DataFrame this]
  (.column_ct this))

(defn nrow [^explojure.dataframe.construct.DataFrame this]
  (.row_ct this))

(defn columns [^explojure.dataframe.construct.DataFrame this]
  (.columns this))

(defn colname-idx [^explojure.dataframe.construct.DataFrame this]
  (.colname_idx this))

(defn rowname-idx [^explojure.dataframe.construct.DataFrame this]
  (.rowname_idx this))

(defn col-vectors [^explojure.dataframe.construct.DataFrame this]
  (map (fn [name c]
         (with-meta c {:name name}))
       (colnames this)
       (columns this)))
  
(defn row-vectors [^explojure.dataframe.construct.DataFrame this]
  (if (= (nrow this) 0)
    (lazy-seq)
    (letfn [(row-fn
              [i]
              (apply vector (for [c (columns this)] (c i))))
            
            (rcr-fn
              [rows]
              (let [i (first rows)
                    remaining (next rows)]
                (lazy-seq (cons (row-fn i) (when (seq remaining) (rcr-fn remaining))))))]
      (rcr-fn (range (nrow this))))))

(defn lookup-names [^explojure.dataframe.construct.DataFrame this
                    axis xs]
  (assert (contains? #{:rows :cols} axis)
          "lookup-names: axis must be either :rows or :cols")
  (let [index (case axis
                :rows (rowname-idx this)
                :cols (colname-idx this))]
    (if (sequential? xs)
      (util/vmap #(get index %) xs)
      (get index xs))))
