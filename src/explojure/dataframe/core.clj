(ns explojure.dataframe.core
  (:require [explojure.dataframe.join :as join]
            [explojure.dataframe.util :as dfu]
            [explojure.dataframe.specifiers :as spec]
            [explojure.dataframe.validation :as valid]
            [explojure.util :as util]
            
            [clojure.set :as s]
            [clojure.test :as t]))

;; Placeholder to allow compilation.
(declare new-dataframe)

(defn sseq?
  "Return true if x is a sequence having only sequences (zero ro more) as children"
  [x]
  (every? identity
          (concat [(sequential? x)]
                  (for [el x]
                    (sequential? el)))))
(defn truthy? [x]
  (if x true false))

(defn falsey? [x]
  (if x false true))

(defn all? [xs]
  (reduce #(and %1 %2)
          xs))

(defn equal-length?
  "REturn true if every sequence x in xs has the same count. Will cause lazy sequences to be evaluated."
  [xs]
  {:pre [(every? sequential? xs)]}
  (apply = (map count xs)))

(defn cmb-cols-vt
  [& args]
  {:pre [(every? sseq? args)
         (equal-length? args)
         (every? equal-length? args)]}
  (util/vmap util/vflatten
             (apply util/vmap vector args)))

(defn cmb-cols-hr [& args]
  {:pre [(every? sseq? args)
         (every? equal-length? args)]}
  (apply util/vconcat args))

(defn collate-keyvals [keyvals]
  {:pre [(sseq? keyvals)
         (or (= (count keyvals) 0)
             (and (= (count (first keyvals)) 2)
                  (equal-length? keyvals)))]}
  (reduce (fn [m [k v]]
            (assoc m k (conj (get m k []) v)))
          {}
          keyvals))

(defn empty-sequential? [x]
  (if (and (sequential? x)
           (= (count x) 0))
    true
    false))

(def conflict-resolution-strategies
  {;; always return left value
   :left (fn [_ _ left _] left)

   ;; always return right value
   :right (fn [_ _ _ right] right)

   ;; return left value if not nil; else right; else nil
   :left-not-nil (fn [_ _ left right]
                   (if left left right))

   ;; return right value if not nil; else left; else nil
   :right-not-nil (fn [_ _ left right]
                    (if right right left))})

(deftype DataFrame
    [colnames    ; a vector of column names, giving column order
     columns     ; a vector of column vectors
     column-ct   ; the number of columns
     colname-idx ; a hash-map of colname => 0-based index
     rownames    ; a vector of row names, in order
     row-ct      ; the number of rows
     rowname-idx ; (optional) a hash-map of rowname => 0-based index
     ]

  dfu/Tabular

  ;; get simple attributes
  (colnames [this] colnames)
  (rownames [this] rownames)
  (ncol [this] column-ct)
  (nrow [this] row-ct)
  (not-nil
   [this]
   (new-dataframe [:column :count]
                  [colnames (vec (for [c columns]
                                 (count
                                  (filter identity
                                          (map #(not (nil? %))
                                               c)))))]))

  (equal?
   [this other]
   "  Uses colnames, columns, and rownames as the basis for equality
  comparison with another DataFrame."
   (and (= colnames (colnames other))
        (= rownames (rownames other))
        (= columns (vec (dfu/col-vectors other)))))

  ;; convert names to 0-based indices
  (lookup-names
   [this axis xs]
   (assert (contains? #{:rows :cols} axis)
           "lookup-names: axis must be either :rows or :cols")
   (let [index (case axis
                 :rows rowname-idx
                 :cols colname-idx)]
     (if (sequential? xs)
       (util/vmap #(get index %) xs)
       (get index xs))))

  ;; get raw data
  (col-vectors
   [this]
   (map (fn [name c]
          (with-meta c {:name name}))
        colnames
        columns))
  
  (row-vectors
   [this]
   (if (= row-ct 0)
     (lazy-seq)
     (letfn [(row-fn
              [i]
              (apply vector (for [c columns] (c i))))

             (rcr-fn
              [rows]
              (let [i (first rows)
                    remaining (next rows)]
                (lazy-seq (cons (row-fn i) (when (seq remaining) (rcr-fn remaining))))))]
       (rcr-fn (range row-ct)))))

  ;; select
  (select-cols-by-index
   [this cols]
   (assert (sequential? cols)
           "select-cols-by-index: column indices must be supplied in a sequential collection.")
   (assert (every? integer? cols)
           "select-cols-by-index: supplied column indices must be integers.")
   (assert (every? #(not (neg? %)) cols)
           "select-cols-by-index: negative column indices not allowed.")
   (assert (every? #(< % column-ct) cols)
           "select-cols-by-index: column indices must be within range [0, ncol-1]")

   (if (= (count cols) 0)
     (do
       (binding [*out* *err*]
         (println "WARNING: selecting 0 columns and, consequently, 0 rows")
         (when rownames
           (println "WARNING: dropping rownames")))
       (new-dataframe [] []))
     (new-dataframe (util/vmap colnames cols)
                    (util/vmap columns cols)
                    rownames)))
  
  (select-rows-by-index
   [this rows]
   (assert (sequential? rows)
           "select-rows-by-index: row indices must be supplied in a sequential collection.")
   (assert (every? integer? rows)
           "select-rows-by-index: supplied row indices must be integers.")
   (assert (every? #(not (neg? %)) rows)
           "select-rows-by-index: negative row indices not allowed.")
   (assert (every? #(< % row-ct) rows)
           "select-rows-by-index: row indices must be within range [0, nrow-1]")
   (if (= (count rows) 0)
     (new-dataframe colnames
                    (util/vrepeat (count colnames) [])
                    (if rownames [] nil))
     (new-dataframe colnames
                    (vec (for [c columns]
                           (util/vmap c rows)))
                    (if rownames
                      (util/vmap rownames rows)
                      nil))))

  ($
   [this col-spec]
   (dfu/$ this col-spec nil))
  
  ($
   [this col-spec row-spec]
   ;; if either or both of the specifiers are nil,
   ;;   don't apply filter to that axis.
   (let [filtered this
         filtered (if (nil? col-spec)
                    filtered
                    (let [col-indices (spec/interpret-spec this :cols col-spec)]
                      (dfu/select-cols-by-index filtered col-indices)))
         ;; if empty sequential was passed as col-spec, then we've
         ;; got an empty dataframe without a rownames field, etc.
         ;; it doesn't make sense to try to filter on the rows.
         filtered (if (or (nil? row-spec)
                          (empty-sequential? col-spec))
                    filtered
                    (let [row-indices (spec/interpret-spec this :rows row-spec)]
                      (dfu/select-rows-by-index filtered row-indices)))]
     (cond
       ;; return resulting dataframe
       (and (spec/multi-specifier? col-spec)
            (spec/multi-specifier? row-spec))
       filtered
       
       ;; handle single row
       (and (spec/multi-specifier? col-spec)
            (not (spec/multi-specifier? row-spec)))
       (first (dfu/row-vectors filtered))

       ;; handle single col
       (and (not (spec/multi-specifier? col-spec))
            (spec/multi-specifier? row-spec))
       (first (dfu/col-vectors filtered))

       ;; return single value
       (and (not (spec/multi-specifier? col-spec))
            (not (spec/multi-specifier? row-spec)))
       (first (first (dfu/col-vectors filtered))))))

  ;; drop
  ;; TODO: The dropping mechanism in these two functions
  ;;   might be more efficient if we used (remove) instead of
  ;;   our (filter #(not (drop-set %)).
  (drop-cols-by-index
   [this cols]
   (assert (sequential? cols)
           "drop-cols-by-index: column indices must be supplied in a sequential collection.")
   (assert (every? integer? cols)
           "drop-cols-by-index: supplied column indices must be integers.")
   (assert (every? #(not (neg? %)) cols)
           "drop-cols-by-index: negative column indices not allowed.")
   (assert (every? #(< % column-ct) cols)
           "select-cols-by-index: col indices must be within range [0, ncol-1]")
   (let [drop-set (set cols)]
     (dfu/select-cols-by-index this
                               (util/vfilter #(not (drop-set %))
                                             (util/vrange column-ct)))))
  
  (drop-rows-by-index
   [this rows]
   (assert (sequential? rows)
           "drop-rows-by-index: row indices must be supplied in a sequential collection.")
   (assert (every? integer? rows)
           "drop-rows-by-index: supplied row indices must be integers.")
   (assert (every? #(not (neg? %)) rows)
           "drop-rows-by-index: negative row indices not allowed.")
   (assert (every? #(< % row-ct) rows)
           "select-rows-by-index: row indices must be within range [0, nrow-1]")
   
   (let [drop-set (set rows)]
     (dfu/select-rows-by-index this
                               (util/vfilter #(not (drop-set %))
                                             (util/vrange row-ct)))))

  ($-
   [this col-spec]
   (dfu/$- this col-spec nil))

  ($-
   [this col-spec row-spec]
   ;; if either or both of the specifiers are nil,
   ;;   don't apply filter to that axis.
   (let [filtered this
         filtered (if (nil? col-spec)
                    filtered
                    (let [col-indices (spec/interpret-spec this :cols col-spec)]
                      (dfu/drop-cols-by-index filtered col-indices)))
         ;; if it's empty after filtering on columns, then we
         ;; have an empty dataframe without a rownames field, etc.
         ;; it doesn't make sense to try to filter on the rows.
         filtered (if (or (nil? row-spec)
                          (= (dfu/ncol filtered) 0))
                    filtered
                    (let [row-indices (spec/interpret-spec this :rows row-spec)]
                      (dfu/drop-rows-by-index filtered row-indices)))]
     filtered))

  (add-col
   [this colname column]
   (let [existing (set colnames)]
     (assert (not (existing colname))
             (str "add-col: column \"" colname "\" already exists"))
     (new-dataframe (conj colnames colname)
                    (conj columns column)
                    rownames)))
  (add-col
   [this add-map]
   (assert (associative? add-map)
           (str "add-col: add-map must be associative. (Received " (type add-map) ".)"))
   (reduce (fn [d [c v]]
             (dfu/add-col d c v))
           this
           (seq add-map)))

  (rename-col
   [this old-colname new-colname]
   (assert (contains? colname-idx old-colname)
           (str "rename-col: cannot rename a column that doesn't exist (" old-colname ")."))
   (assert (not (contains? colname-idx new-colname))
           (str "rename-col: cannot rename column to something that already exists (" new-colname ")."))
   (let [old-cn-idx (get colname-idx old-colname)]
     (new-dataframe (assoc colnames old-cn-idx new-colname)
                    columns
                    rownames)))

  (rename-col
   [this rename-map]
   (assert (associative? rename-map)
           (str "rename-col: rename-map must be associative. (Received " (type rename-map) ".)"))
   (reduce (fn [d [o n]]
             (dfu/rename-col d o n))
           this
           (seq rename-map)))

  (replace-col
   [this colname column]
   (assert (contains? colname-idx colname)
           (str "replace-col: Cannot replace a column that doesn't exist (" colname ")."))
   (let [cn-idx (get colname-idx colname)]
     (new-dataframe colnames
                    (assoc columns cn-idx column)
                    rownames)))

  (replace-col
   [this replace-map]
   (assert (associative? replace-map)
           (str "replace-col: replace-map must be associative. (Received " (type replace-map) ".)"))
   (reduce (fn [d [oc nd]]
             (dfu/replace-col d oc nd))
           this
           (seq replace-map)))

  (set-col
   [this colname column]
   (if (contains? colname-idx colname)
     (dfu/replace-col this colname column)
     (dfu/add-col this colname column)))

  (set-col
   [this set-map]
   (assert (associative? set-map)
           (str "set-col: set-map must be associative. (Received " (type set-map) ".)"))
   (reduce (fn [df [colname column]]
             (dfu/set-col df colname column))
           this
           (seq set-map)))

  (set-colnames
   [this new-colnames]
   (new-dataframe new-colnames
                  columns
                  rownames))

  (set-rownames
   [this new-rownames]
   (new-dataframe colnames
                  columns
                  new-rownames))


  
  (join-frames
   [this right on join-type]
   (dfu/join-frames this right on on join-type))

  (join-frames
   [this right left-on right-on join-type]
   (let [[lo-sel lc-sel rc-sel ro-sel]
         (join/join-selections (apply (partial map vector) (dfu/col-vectors
                                                            (dfu/$ this left-on nil)))
                               (apply (partial map vector) (dfu/col-vectors
                                                            (dfu/$ right right-on nil))))

         lo (dfu/$ this nil lo-sel)
         ro (dfu/$ this nil lc-sel)]))

  clojure.lang.IFn

  ;; invoke DataFrame as function to do a ($) select operation
  (invoke
   [this col-spec row-spec]
   (dfu/$ this col-spec row-spec))
  
  (invoke
   [this col-spec]
   (dfu/$ this col-spec)))

(defn empty-df
  "Create a dataframe with the specified column names and nrow nils per column."
  [colnames nrow]
  (new-dataframe colnames
                 (dfu/nil-cols (count colnames) nrow)))

(defn new-dataframe
  ([cols vecs]
   (new-dataframe cols vecs nil))
  
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
     (let [rowname-idx (if (not (nil? rownames))
                         (do
                           ;; ensure rowname assumptions
                           (valid/validate-rownames rownames row-ct)
                           (let [rowname-idx (reduce (fn [m [k i]]
                                                       (assoc m k i))
                                                     {}
                                                     (map vector
                                                          rownames
                                                          (range row-ct)))]
                             rowname-idx))
                         nil)]
       ;; create a new DataFrame
       (new explojure.dataframe.core.DataFrame
            cols
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

(defn conj-cols [left right]
  (cond (or (nil? right)
            (= (dfu/ncol right) 0))
        left

        (or (nil? left)
            (= (dfu/ncol left) 0))
        right
        


        :default
        (let [left-colnames (dfu/colnames left)
              right-colnames (dfu/colnames right)
              left-rownames (dfu/rownames left)
              right-rownames (dfu/rownames right)
              
              colname-conflicts (s/intersection (set left-colnames)
                                                (set right-colnames))
              equal-nrow (= (dfu/nrow left)
                            (dfu/nrow right))

              left-has-rownames (not (nil? left-rownames))
              right-has-rownames (not (nil? right-rownames))
              
              align-rownames (and left-has-rownames
                                  right-has-rownames)]
          
          (assert (= (count colname-conflicts) 0)
                  (str "conj-cols: colnames must be free of conflicts (" colname-conflicts "). Consider using (merge-frames) if appropriate."))
          
          (if align-rownames
            ;; if both DFs have rownames
            (let [[left-only in-common left-and-common right-only]
                  (util/venn-components left-rownames
                                        right-rownames)
                  
                  combined-colnames
                  (util/vconcat left-colnames right-colnames)

                  combined-columns
                  (cmb-cols-vt (cmb-cols-hr (dfu/col-vectors (dfu/$ left nil left-only))
                                            (dfu/nil-cols (dfu/ncol right) (count left-only)))
                               (cmb-cols-hr (dfu/col-vectors (dfu/$ left nil in-common))
                                            (dfu/col-vectors (dfu/$ right nil in-common)))
                               (cmb-cols-hr (dfu/nil-cols (dfu/ncol left) (count right-only))
                                            (dfu/col-vectors (dfu/$ right nil right-only))))
                  
                  combined-rownames
                  (util/vconcat left-only in-common right-only)]
              
              (dfu/$ (new-dataframe combined-colnames
                                    combined-columns
                                    combined-rownames)
                     nil
                     (concat left-and-common right-only)))

            ;; if one or none of the DFs have rownames
            (do
              (assert equal-nrow
                      "conj-cols: if both dataframes do not each have rownames, the two dataframes must have the same number of rows.")
              (let [combined-colnames (util/vconcat left-colnames
                                                    right-colnames)
                    
                    combined-columns (util/vconcat (dfu/col-vectors left)
                                                   (dfu/col-vectors right))
                    
                    ;; at this point we know it is not true that both DFs have rownames
                    ;; so use the one that has them.  Or nil if neither has them.
                    use-rownames (if (dfu/rownames left)
                                   (dfu/rownames left)
                                   (dfu/rownames right))
                    
                    [left-only in-common left-and-common right-only]
                    (util/venn-components left-colnames
                                          right-colnames)]

                (new-dataframe combined-colnames
                               combined-columns
                               use-rownames)))))))

(defn conj-rows [top bottom]
  (cond (or (nil? bottom)
            (= (dfu/ncol bottom) 0))
        top

        (or (nil? top)
            (= (dfu/ncol top) 0))
        bottom

        :default
        (let [top-rownames (dfu/rownames top)
              bottom-rownames (dfu/rownames bottom)
              
              top-has-rownames (not (nil? top-rownames))
              bottom-has-rownames (not (nil? bottom-rownames))

              both-have-rownames (and top-has-rownames
                                      bottom-has-rownames)
              both-no-rownames (and (not top-has-rownames)
                                    (not bottom-has-rownames))
              
              rowname-conflicts (s/intersection (set top-rownames)
                                                (set bottom-rownames))]
          
          (assert (or both-have-rownames both-no-rownames)
                  (str "conj-rows: DataFrames must both have rownames, or they must both not have rownames in order to maintain the integrity of rowname semantics."))
          (when both-have-rownames
            (assert (= (count rowname-conflicts) 0)
                    (str "conj-rows: rownames must be free of conflicts (" rowname-conflicts "). Consider using (merge-frames) if appropriate.")))

          (let [top-colnames (dfu/colnames top)
                bottom-colnames (dfu/colnames bottom)

                [top-only in-common top-and-common bottom-only]
                (util/venn-components top-colnames
                                      bottom-colnames)
                cmb-colnames (util/vconcat top-only in-common bottom-only)
                
                cmb-columns (cmb-cols-vt
                             (cmb-cols-hr (dfu/col-vectors (dfu/$ top top-only nil))
                                          (dfu/col-vectors (dfu/$ top in-common nil))
                                          (dfu/nil-cols (count bottom-only)
                                                        (dfu/nrow top)))
                             (cmb-cols-hr (dfu/nil-cols (count top-only)
                                                        (dfu/nrow bottom))
                                          (dfu/col-vectors (dfu/$ bottom in-common nil))
                                          (dfu/col-vectors (dfu/$ bottom bottom-only nil))))
                
                cmb-rownames (if both-have-rownames
                               (util/vconcat top-rownames
                                             bottom-rownames)
                               nil)]
            
            (dfu/$ (new-dataframe cmb-colnames
                                  cmb-columns
                                  cmb-rownames)
                   (concat top-and-common bottom-only)
                   nil)))))

(defn merge-frames [df1 df2 resolution-fn]
  (cond (or (nil? df2)
            (= (dfu/ncol df2) 0))
        df1

        (or (nil? df1)
            (= (dfu/ncol df1) 0))
        df2

        :default
        (let [df1-colnames (dfu/colnames df1)
              df2-colnames (dfu/colnames df2)
              df1-rownames (dfu/rownames df1)
              df2-rownames (dfu/rownames df2)]

          ;; some input validation
          (assert (and df1-colnames df2-colnames
                       df1-rownames df2-rownames)
                  (str "merge: both DataFrames must have both rownames and colnames:\n"
                       "df1 colnames: " (if df1-colnames "supplied" "missing") "\n"
                       "df1 rownames: " (if df1-rownames "supplied" "missing") "\n"
                       "df2 colnames: " (if df2-colnames "supplied" "missing") "\n"
                       "df2 rownames: " (if df2-rownames "supplied" "missing")))

          (let [;; set components
                [rn-df1-only rn-in-common rn-df1-and-common rn-df2-only]
                (util/venn-components df1-rownames df2-rownames)
                [cn-df1-only cn-in-common cn-df1-and-common cn-df2-only]
                (util/venn-components df1-colnames df2-colnames)

                ;; overlap / conflict
                conflict-columns (for [c cn-in-common]
                                   [c rn-in-common])

                ;; resolve conflicts
                resolved (vec (for [[col rownames] conflict-columns]
                                (util/vmap #(apply resolution-fn %)
                                           (map vector
                                                (repeat col)
                                                rownames
                                                (dfu/$ df1 col rownames)
                                                (dfu/$ df2 col rownames)))))

                combined-columns
                (cmb-cols-vt
                 (cmb-cols-hr (dfu/col-vectors (dfu/$ df1 cn-df1-only rn-df1-only))
                              (dfu/col-vectors (dfu/$ df1 cn-in-common rn-df1-only))
                              (dfu/nil-cols (count cn-df2-only) (count rn-df1-only)))

                 (cmb-cols-hr (dfu/col-vectors (dfu/$ df1 cn-df1-only rn-in-common))
                              resolved
                              (dfu/col-vectors (dfu/$ df2 cn-df2-only rn-in-common)))
                 
                 (cmb-cols-hr (dfu/nil-cols (count cn-df1-only) (count rn-df2-only))
                              (dfu/col-vectors (dfu/$ df2 cn-in-common rn-df2-only))
                              (dfu/col-vectors (dfu/$ df2 cn-df2-only rn-df2-only))))
                
                combined-colnames (util/vconcat cn-df1-only cn-in-common cn-df2-only)
                
                combined-rownames (util/vconcat rn-df1-only rn-in-common rn-df2-only)]
            
            (dfu/$ (new-dataframe combined-colnames
                                  combined-columns
                                  combined-rownames)
                   (concat cn-df1-and-common cn-df2-only)
                   (concat rn-df1-and-common rn-df2-only))))))






