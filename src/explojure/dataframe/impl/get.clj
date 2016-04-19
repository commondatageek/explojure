(ns explojure.dataframe.impl.get
  (:require [explojure.dataframe.construct]
            [explojure.util :as util]))

(defn colnames [this]
  (.colnames this))

(defn rownames [this]
  (.rownames this))

(defn ncol [this]
  (.column_ct this))

(defn nrow [this]
  (.row_ct this))

(defn columns [this]
  (.columns this))

(defn colname-idx [this]
  (.colname_idx this))

(defn rowname-idx [this]
  (.rowname_idx this))

(defn col-vectors [this]
  (map (fn [name c]
         (with-meta c {:name name}))
       (colnames this)
       (columns this)))
  
(defn row-vectors [this]
  (apply (partial map vector)
         (columns this)))

(defn lookup-names [this axis xs]
  (assert (contains? #{:rows :cols} axis)
          "lookup-names: axis must be either :rows or :cols")
  (let [index (case axis
                :rows (rowname-idx this)
                :cols (colname-idx this))]
    (if (sequential? xs)
      (util/vmap #(get index %) xs)
      (get index xs))))
