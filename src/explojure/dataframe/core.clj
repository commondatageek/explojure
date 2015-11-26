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

(defn cmb-cols-vt [& args]
  (util/vmap util/vflatten
             (apply util/vmap vector args)))

(defn cmb-cols-hr [& args]
  (apply util/vconcat args))

(defn collate-keyvals [keyvals]
  (reduce (fn [m [k v]]
            (assoc m k (conj (get m k []) v)))
          {}
          keyvals))

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
         (println "WARNING: selecting 0 columns")
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
                    (util/vmap rownames rows))))
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
         filtered (if (nil? row-spec)
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
   (if (= (dfu/ncol this) 0)
     this
     (do
       (assert (sequential? cols)
               "drop-cols-by-index: column indices must be supplied in a sequential collection.")
       (assert (every? integer? cols)
               "drop-cols-by-index: supplied column indices must be integers.")
       (assert (every? #(not (neg? %)) cols)
               "drop-cols-by-index: negative column indices not allowed.")
       
       (let [drop-set (set cols)]
         (dfu/select-cols-by-index this
                               (util/vfilter #(not (drop-set %))
                                             (util/vrange column-ct)))))))
  (drop-rows-by-index
   [this rows]
   (if (= (dfu/nrow this) 0)
     this
     (do
       (assert (sequential? rows)
               "drop-rows-by-index: row indices must be supplied in a sequential collection.")
       (assert (every? integer? rows)
               "drop-rows-by-index: supplied row indices must be integers.")
       (assert (every? #(not (neg? %)) rows)
               "drop-rows-by-index: negative row indices not allowed.")
       
       (let [drop-set (set rows)]
         (dfu/select-rows-by-index this
                               (util/vfilter #(not (drop-set %))
                                             (util/vrange row-ct)))))))

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
         filtered (if (nil? row-spec)
                    filtered
                    (let [row-indices (spec/interpret-spec this :rows row-spec)]
                      (dfu/drop-rows-by-index filtered row-indices)))]
     filtered))

  (add-col
   [this colname column]
   (new-dataframe (conj colnames colname)
                  (conj columns column)
                  rownames))
  (add-col
   [this add-map]
   (assert (associative? add-map)
           (str "add-col: add-map must be associative. (Received " (type add-map) ".)"))
   (new-dataframe (util/vconcat colnames (keys add-map))
                  (util/vconcat columns (vals add-map))
                  rownames))

  (rename-col
   [this old-colname new-colname]
   (assert (contains? colname-idx old-colname)
           (str "rename-col: Cannot replace a column that doesn't exist (" old-colname ")."))
   (let [old-cn-idx (get colname-idx old-colname)]
     (new-dataframe (assoc colnames old-cn-idx new-colname)
                    columns
                    rownames)))

  (rename-col
   [this rename-map]
   (assert (associative? rename-map)
           (str "rename-col: rename-map must be associative. (Received " (type rename-map) ".)"))
   (let [non-cols (util/vfilter #(not (contains? colname-idx %))
                                (keys rename-map))]
     (assert (= (count non-cols) 0)
             (str "rename-col: Cannot rename columns that don't exist: " non-cols)))
   (new-dataframe (reduce (fn [v [old-colname new-colname]]
                            (let [old-cn-idx (get colname-idx old-colname)]
                              (assoc v old-cn-idx new-colname)))
                          colnames
                          (seq rename-map))
                  columns
                  rownames))

  (replace-col
   [this colname column]
   (assert (contains? colname-idx colname)
           (str "replace-col: Cannot replace a column that doesn't exist (" colname ")."))
   (let [cn-idx (get colname-idx colname)]
     (new-dataframe colnames
                    (assoc columns cn-idx column))))

  (replace-col
   [this replace-map]
   (assert (associative? replace-map)
           (str "replace-col: replace-map must be associative. (Received " (type replace-map) ".)"))
   (let [non-cols (util/vfilter #(not (contains? colname-idx %))
                                (keys replace-map))]
     (assert (= (count non-cols) 0)
             (str "replace-col: Cannot replace columns that don't exist: " non-cols)))
   (new-dataframe colnames
                  (reduce (fn [v [colname new-column]]
                            (let [cn-idx (get colname-idx colname)]
                              (assoc v cn-idx new-column)))
                          columns
                          (seq replace-map))
                  rownames))

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

  (conj-cols
   [this right]
   (let [left-colnames (dfu/colnames this)
         right-colnames (dfu/colnames right)
         left-rownames (dfu/rownames this)
         right-rownames (dfu/rownames right)
         
         colname-conflicts (s/intersection (set left-colnames)
                                           (set right-colnames))
         equal-nrow (= (dfu/nrow this)
                       (dfu/nrow right))

         left-has-rownames (not (nil? left-rownames))
         right-has-rownames (not (nil? right-rownames))
         
         align-rownames (and left-has-rownames
                             right-has-rownames)]
     
     (assert (= (count colname-conflicts) 0)
             (str "conj-cols: colnames must be free of conflicts (" colname-conflicts "). Consider using (merge-frames) if appropriate."))
     
     (if align-rownames
       ;; if both DFs have rownames
       (let [[left-only in-common left-and-common right-only] (util/venn-components left-rownames
                                                                               right-rownames)
             
             combined-colnames
             (util/vconcat left-colnames right-colnames)

             combined-columns
             (cmb-cols-vt (cmb-cols-hr (dfu/col-vectors (dfu/$ this nil left-only))
                                       (dfu/nil-cols (dfu/ncol right) (count left-only)))
                          (cmb-cols-hr (dfu/col-vectors (dfu/$ this nil in-common))
                                       (dfu/col-vectors (dfu/$ right nil in-common)))
                          (cmb-cols-hr (dfu/nil-cols (dfu/ncol this) (count right-only))
                                       (dfu/col-vectors (dfu/$ right nil right-only))))
             
             combined-rownames
             (util/vconcat left-only in-common right-only)]
         
         (dfu/$ (new-dataframe combined-colnames
                           combined-columns
                           combined-rownames)
            (concat left-and-common right-only)
            nil))

       ;; if one or none of the DFs have rownames
       (let [combined-colnames (util/vconcat left-colnames
                                             right-colnames)

             combined-columns (util/vconcat (dfu/col-vectors this)
                                            (dfu/col-vectors right))

             ;; at this point we know it is not true that both DFs have rownames
             ;; so use the one that has them.  Or nil if neither has them.
             use-rownames (if (dfu/rownames this)
                            (dfu/rownames this)
                            (dfu/rownames right))

             [left-only in-common left-and-common right-only] (util/venn-components left-colnames
                                                                               right-colnames)]
         (dfu/$ (new-dataframe combined-colnames
                           combined-columns
                           use-rownames)
            (concat left-and-common right-only)
            nil)))))

  (conj-rows
   [this bottom]
   (let [top-rownames (dfu/rownames this)
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
     (assert (= (count rowname-conflicts) 0)
             (str "conj-rows: rownames must be free of conflicts (" rowname-conflicts "). Consider using (merge-frames) if appropriate."))

       (let [top-colnames (dfu/colnames this)
             bottom-colnames (dfu/colnames bottom)

             [top-only in-common top-and-common bottom-only] (util/venn-components top-colnames
                                                                              bottom-colnames)
             cmb-colnames (util/vconcat top-only in-common bottom-only)
             
             cmb-columns (cmb-cols-vt
                          (cmb-cols-hr (dfu/col-vectors (dfu/$ this top-only nil))
                                           (dfu/col-vectors (dfu/$ this in-common nil))
                                           (dfu/nil-cols (count bottom-only)
                                                     (dfu/nrow this)))
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
            nil))))

  (merge-frames
   [this right resolution-fn]
   (let [left-colnames (dfu/colnames this)
         right-colnames (dfu/colnames right)
         left-rownames (dfu/rownames this)
         right-rownames (dfu/rownames right)]

     ;; some input validation
     (assert (and left-colnames right-colnames
                  left-rownames right-rownames)
             (str "merge: both DataFrames must have both rownames and colnames:\n"
                  "left colnames: " (if left-colnames "supplied" "missing") "\n"
                  "left rownames: " (if left-rownames "supplied" "missing") "\n"
                  "right colnames: " (if right-colnames "supplied" "missing") "\n"
                  "right rownames: " (if right-rownames "supplied" "missing")))

     (let [;; set components
           [rn-left-only rn-in-common rn-left-and-common rn-right-only]
           (util/venn-components left-rownames right-rownames)
           [cn-left-only cn-in-common cn-left-and-common cn-right-only]
           (util/venn-components left-colnames right-colnames)

           ;; overlap / conflict
           conflict-columns (for [c cn-in-common]
                              [c rn-in-common])

           ;; resolve conflicts
           resolved (vec (for [[col rownames] conflict-columns]
                           (util/vmap #(apply resolution-fn %)
                                      (map vector
                                           (repeat col)
                                           rownames
                                           (dfu/$ this col rownames)
                                           (dfu/$ right col rownames)))))

           combined-columns
           (cmb-cols-vt
            (cmb-cols-hr (dfu/col-vectors (dfu/$ this cn-left-only rn-left-only))
                         (dfu/col-vectors (dfu/$ this cn-in-common rn-left-only))
                         (dfu/nil-cols (count cn-right-only) (count rn-left-only)))
            
            (cmb-cols-hr (dfu/col-vectors (dfu/$ this cn-left-only rn-in-common))
                         resolved
                         (dfu/col-vectors (dfu/$ right cn-right-only rn-in-common)))
            
            (cmb-cols-hr (dfu/nil-cols (count cn-left-only) (count rn-right-only))
                         (dfu/col-vectors (dfu/$ right cn-in-common rn-right-only))
                         (dfu/col-vectors (dfu/$ right cn-right-only rn-right-only))))
           
           combined-colnames (util/vconcat cn-left-only cn-in-common cn-right-only)
           
           combined-rownames (util/vconcat rn-left-only rn-in-common rn-right-only)]
       
       (dfu/$ (new-dataframe combined-colnames
                         combined-columns
                         combined-rownames)
          (concat cn-left-and-common cn-right-only)
          (concat rn-left-and-common rn-right-only)))))

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
