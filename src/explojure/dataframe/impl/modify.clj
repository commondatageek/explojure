(ns explojure.dataframe.impl.modify
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.get :as raw]))

(defn add-col
  ([^explojure.dataframe.construct.DataFrame this
    colname column]
   (let [colnames (raw/colnames this)
         existing (set colnames)
         columns  (raw/columns this)
         rownames (raw/rownames this)]
     (assert (not (existing colname))
             (str "add-col: column \"" colname "\" already exists"))
     (ctor/new-dataframe (conj colnames colname)
                         (conj columns column)
                         rownames)))
  
  ([^explojure.dataframe.construct.DataFrame this
    add-map]
   (assert (associative? add-map)
           (str "add-col: add-map must be associative. (Received " (type add-map) ".)"))
   (reduce (fn [d [c v]]
             (add-col d c v))
           this
           (seq add-map))))


(defn rename-col
  ([^explojure.dataframe.construct.DataFrame this
    old-colname new-colname]
   (let [colname-idx (raw/colname-idx this)
         colnames (raw/colnames this)
         columns (raw/columns this)
         rownames (raw/rownames this)]
     (assert (contains? colname-idx old-colname)
             (str "rename-col: cannot rename a column that doesn't exist (" old-colname ")."))
     (assert (not (contains? colname-idx new-colname))
             (str "rename-col: cannot rename column to something that already exists (" new-colname ")."))
     (let [old-cn-idx (get colname-idx old-colname)]
       (ctor/new-dataframe (assoc colnames old-cn-idx new-colname)
                           columns
                           rownames))))
  
  ([^explojure.dataframe.construct.DataFrame this
    rename-map]
   (assert (associative? rename-map)
           (str "rename-col: rename-map must be associative. (Received " (type rename-map) ".)"))
   (reduce (fn [d [o n]]
             (rename-col d o n))
           this
           (seq rename-map))))

(defn replace-col
  ([^explojure.dataframe.construct.DataFrame this
    colname column]
   (let [colname-idx (raw/colname-idx this)
         columns  (raw/columns this)
         colnames (raw/colnames this)
         rownames (raw/rownames this)]
     (assert (contains? colname-idx colname)
             (str "replace-col: Cannot replace a column that doesn't exist (" colname ")."))
     (let [cn-idx (get colname-idx colname)]
       (ctor/new-dataframe colnames
                           (assoc columns cn-idx column)
                           rownames))))

  ([^explojure.dataframe.construct.DataFrame this
    replace-map]
   (assert (associative? replace-map)
           (str "replace-col: replace-map must be associative. (Received " (type replace-map) ".)"))
   (reduce (fn [d [oc nd]]
             (replace-col d oc nd))
           this
           (seq replace-map))))



(defn set-col
  ([^explojure.dataframe.construct.DataFrame this
    colname column]
   (let [colname-idx (raw/colname-idx this)]
     (if (contains? colname-idx colname)
       (replace-col this colname column)
       (add-col this colname column))))
  
  ([^explojure.dataframe.construct.DataFrame this
    set-map]
   (assert (associative? set-map)
           (str "set-col: set-map must be associative. (Received " (type set-map) ".)"))
   (reduce (fn [df [colname column]]
             (set-col df colname column))
           this
           (seq set-map))))

(defn set-colnames [^explojure.dataframe.construct.DataFrame this
                    new-colnames]
  (ctor/new-dataframe new-colnames
                      (raw/columns this)
                      (raw/rownames this)))

(defn set-rownames [^explojure.dataframe.construct.DataFrame this
                    new-rownames]
  (ctor/new-dataframe (raw/colnames this)
                      (raw/columns this)
                      new-rownames))
