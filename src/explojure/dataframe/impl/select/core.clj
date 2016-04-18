(ns explojure.dataframe.impl.select.core
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.get :as raw]
            [explojure.dataframe.impl.select.specify :as spec]
            [explojure.dataframe.util :as dfu]
            [explojure.util :as util]))

(defn select-cols-by-index [this cols]
  (assert (sequential? cols)
          "select-cols-by-index: column indices must be supplied in a sequential collection.")
  (assert (every? integer? cols)
          "select-cols-by-index: supplied column indices must be integers.")
  (assert (every? #(not (neg? %)) cols)
          "select-cols-by-index: negative column indices not allowed.")
  (assert (every? #(< % (raw/ncol this)) cols)
          "select-cols-by-index: column indices must be within range [0, ncol-1]")

  (let [colnames (raw/colnames this)
        columns  (raw/columns this)
        rownames (raw/rownames this)]
    (if (= (count cols) 0)
      (do
        (binding [*out* *err*]
          (println "WARNING: selecting 0 columns and, consequently, 0 rows")
          (when (raw/rownames this)
            (println "WARNING: dropping rownames")))
        (ctor/new-dataframe [] []))
      (ctor/new-dataframe (util/vmap colnames cols)
                          (util/vmap columns cols)
                          rownames))))
  
(defn select-rows-by-index [this rows]
  (assert (sequential? rows)
          "select-rows-by-index: row indices must be supplied in a sequential collection.")
  (assert (every? integer? rows)
          "select-rows-by-index: supplied row indices must be integers.")
  (assert (every? #(not (neg? %)) rows)
          "select-rows-by-index: negative row indices not allowed.")
  (assert (every? #(< % (raw/nrow this)) rows)
          "select-rows-by-index: row indices must be within range [0, nrow-1]")
  
  (let [colnames (raw/colnames this)
        columns  (raw/columns this)
        rownames (raw/rownames this)]
    (if (= (count rows) 0)
      (ctor/new-dataframe colnames
                          (util/vrepeat (count colnames) [])
                          (if rownames [] nil))
      (ctor/new-dataframe colnames
                          (vec (for [c columns]
                                 (util/vmap c rows)))
                          (if rownames
                            (util/vmap rownames rows)
                            nil)))))

(defn $
  ([this col-spec]
   ($ this col-spec nil))
  ([this col-spec row-spec]
   ;; if either or both of the specifiers are nil,
   ;;   don't apply filter to that axis.
   (let [filtered this
         filtered (if (nil? col-spec)
                    filtered
                    (let [col-indices (spec/interpret-spec this :cols col-spec)]
                      (select-cols-by-index filtered col-indices)))
         ;; if empty sequential was passed as col-spec, then we've
         ;; got an empty dataframe without a rownames field, etc.
         ;; it doesn't make sense to try to filter on the rows.
         filtered (if (or (nil? row-spec)
                          (dfu/empty-sequential? col-spec))
                    filtered
                    (let [row-indices (spec/interpret-spec this :rows row-spec)]
                      (select-rows-by-index filtered row-indices)))]
     (cond
       ;; return resulting dataframe
       (and (spec/multi-specifier? col-spec)
            (spec/multi-specifier? row-spec))
       filtered
       
       ;; handle single row
       (and (spec/multi-specifier? col-spec)
            (not (spec/multi-specifier? row-spec)))
       (first (raw/row-vectors filtered))

       ;; handle single col
       (and (not (spec/multi-specifier? col-spec))
            (spec/multi-specifier? row-spec))
       (first (raw/col-vectors filtered))

       ;; return single value
       (and (not (spec/multi-specifier? col-spec))
            (not (spec/multi-specifier? row-spec)))
       (first (first (raw/col-vectors filtered)))))))

  

  ;; drop
  ;; TODO: The dropping mechanism in these two functions
  ;;   might be more efficient if we used (remove) instead of
  ;;   our (filter #(not (drop-set %)).
(defn drop-cols-by-index [this cols]
  (assert (sequential? cols)
          "drop-cols-by-index: column indices must be supplied in a sequential collection.")
  (assert (every? integer? cols)
          "drop-cols-by-index: supplied column indices must be integers.")
  (assert (every? #(not (neg? %)) cols)
          "drop-cols-by-index: negative column indices not allowed.")
  (assert (every? #(< % (raw/ncol this)) cols)
          "select-cols-by-index: col indices must be within range [0, ncol-1]")
  (let [drop-set (set cols)]
    (select-cols-by-index this
                          (util/vfilter #(not (drop-set %))
                                        (util/vrange (raw/ncol this))))))
  
(defn drop-rows-by-index [this rows]
  (assert (sequential? rows)
          "drop-rows-by-index: row indices must be supplied in a sequential collection.")
  (assert (every? integer? rows)
          "drop-rows-by-index: supplied row indices must be integers.")
  (assert (every? #(not (neg? %)) rows)
          "drop-rows-by-index: negative row indices not allowed.")
  (assert (every? #(< % (raw/nrow this)) rows)
          "select-rows-by-index: row indices must be within range [0, nrow-1]")
   
   (let [drop-set (set rows)]
     (select-rows-by-index this
                           (util/vfilter #(not (drop-set %))
                                         (util/vrange (raw/nrow this))))))

(defn $-
  ([this col-spec]
   ($- this col-spec nil))

  ([this col-spec row-spec]
   ;; if either or both of the specifiers are nil,
   ;;   don't apply filter to that axis.
   (let [filtered this
         filtered (if (nil? col-spec)
                    filtered
                    (let [col-indices (spec/interpret-spec this :cols col-spec)]
                      (drop-cols-by-index filtered col-indices)))
         ;; if it's empty after filtering on columns, then we
         ;; have an empty dataframe without a rownames field, etc.
         ;; it doesn't make sense to try to filter on the rows.
         filtered (if (or (nil? row-spec)
                          (= (raw/ncol filtered) 0))
                    filtered
                    (let [row-indices (spec/interpret-spec this :rows row-spec)]
                      (drop-rows-by-index filtered row-indices)))]
     filtered)))

(defn drop-duplicates
  ([this]
   (drop-duplicates this nil first))

  ([this col-spec]
   (drop-duplicates this col-spec first))

  ([this col-spec keep-fn]
   (let [col-spec (if (and (not (nil? col-spec))
                           (not (sequential? col-spec)))
                    [col-spec]
                    col-spec)
         key-map (util/index-seq
                  (raw/row-vectors
                   ($ this col-spec)))
         keep-rows (sort
                    (flatten
                     (map keep-fn
                          (vals key-map))))]
     ($ this nil keep-rows))))
