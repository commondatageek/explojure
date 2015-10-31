(ns explojure.dataframe.core
  (:require [explojure.dataframe.validation :as valid]
            [explojure.dataframe.join :as join]            
            [explojure.util :as util]
            [clojure.set :as s]
            [clojure.test :as t]))

;; Placeholder to allow compilation.
(declare new-dataframe)
(declare interpret-spec)

(defn multi-selector? [x]
  (or (sequential? x)
      (nil? x)
      (fn? x)))

(defn nil-cols [ncol nrow]
  (->> nil
       (util/vrepeat nrow)
       (util/vrepeat ncol)))

(defn venn-components [left right]
  "Provide left-only, in-common, left-and-common, and right-only components of
   left and right sets. In left-only and right-only components, preserve order
   from left and right.  For in-common, use left order."
  (let [left-set (set left)
        right-set (set right)

        [left-only in-common left-and-common]
        (reduce (fn [[left-only in-common left-and-common] val]
                  (if (nil? (get right-set val))
                    [(conj left-only val) in-common (conj left-and-common val)]
                    [left-only (conj in-common val) (conj left-and-common val)]))
                [[] [] []]
                left)

        right-only (reduce (fn [right-only val]
                             (if (nil? (get left-set val))
                               (conj right-only val)
                               right-only))
                           []
                           right)]
    [left-only in-common left-and-common right-only])  )

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

(defprotocol Tabular
  "The Tabular protocol represents tabular data.  (Rows and columns)."
  (colnames [this])
  (rownames [this])
  (ncol [this])
  (nrow [this])
  (not-nil [this])

  (equal? [this other])

  (lookup-names [this axis xs])

  (col-vectors [this])
  (row-vectors [this])

  ;; select
  (select-cols-by-index [this cols])
  (select-rows-by-index [this rows])
  ($ [this col-spec]
     [this col-spec row-spec])

  ;; drop
  (drop-cols-by-index [this cols])
  (drop-rows-by-index [this cols])
  ($- [this col-spec]
      [this col-spec row-spec])

  (add-col [this colname column]
           [this add-map])

  (rename-col [this old-colname new-colname]
              [this rename-map])

  (replace-col [this colname column]
               [this replace-map])

  (set-col [this colname column]
           [this set-map])
  
  (set-colnames [this new-colnames])
  (set-rownames [this new-rownames])

  ;; combine dataframes
  (conj-cols [this right])
  (conj-rows [this bottom])
  (merge-frames [this other resolution-fn])
  (join-frames [this right on join-type]
               [this right left-on right-on join-type]))

(deftype DataFrame
    [colnames    ; a vector of column names, giving column order
     columns     ; a vector of column vectors
     column-ct   ; the number of columns
     colname-idx ; a hash-map of colname => 0-based index
     rownames    ; a vector of row names, in order
     row-ct      ; the number of rows
     rowname-idx ; (optional) a hash-map of rowname => 0-based index
     ]

  clojure.lang.IFn

  ;; invoke DataFrame as function to do a ($) select operation
  (invoke
   [this col-spec row-spec]
   ($ this col-spec row-spec))
  
  (invoke
   [this col-spec]
   ($ this col-spec))

  Tabular

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
   (and (= colnames (explojure.dataframe.core/colnames other))
        (= rownames (explojure.dataframe.core/rownames other))
        (= columns (vec (col-vectors other)))))

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
   (if (= (ncol this) 0)
     this
     (do
       (assert (sequential? cols)
               "select-cols-by-index: column indices must be supplied in a sequential collection.")
       (assert (every? integer? cols)
               "select-cols-by-index: supplied column indices must be integers.")
       (assert (every? #(not (neg? %)) cols)
               "select-cols-by-index: negative column indices not allowed.")
       
       (new-dataframe (util/vmap colnames cols)
                      (util/vmap columns cols)
                      (if (> (count cols) 0)
                        rownames
                        nil)))))
  
  (select-rows-by-index
   [this rows]
   (if (= (nrow this) 0)
     this
     (do
       (assert (sequential? rows)
               "select-rows-by-index: row indices must be supplied in a sequential collection.")
       (assert (every? integer? rows)
               "select-rows-by-index: supplied row indices must be integers.")
       (assert (every? #(not (neg? %)) rows)
               "select-rows-by-index: negative row indices not allowed.")
       
       (new-dataframe colnames
                      (apply vector
                             (for [c columns]
                               (util/vmap c rows)))
                      (if (> (count rows) 0)
                        (util/vmap rownames rows)
                        nil)))))
  ($
   [this col-spec]
   ($ this col-spec nil))
  
  ($
   [this col-spec row-spec]
   ;; if either or both of the specifiers are nil,
   ;;   don't apply filter to that axis.
   (let [filtered this
         filtered (if (nil? col-spec)
                    filtered
                    (let [col-indices (interpret-spec this :cols col-spec)]
                      (select-cols-by-index filtered col-indices)))
         filtered (if (nil? row-spec)
                    filtered
                    (let [row-indices (interpret-spec this :rows row-spec)]
                      (select-rows-by-index filtered row-indices)))]
     (cond
       ;; return resulting dataframe
       (and (multi-selector? col-spec)
            (multi-selector? row-spec))
       filtered
       
       ;; handle single row
       (and (multi-selector? col-spec)
            (not (multi-selector? row-spec)))
       (first (row-vectors filtered))

       ;; handle single col
       (and (not (multi-selector? col-spec))
            (multi-selector? row-spec))
       (first (col-vectors filtered))

       ;; return single value
       (and (not (multi-selector? col-spec))
            (not (multi-selector? row-spec)))
       (first (first (col-vectors filtered))))))

  ;; drop
  ;; TODO: The dropping mechanism in these two functions
  ;;   might be more efficient if we used (remove) instead of
  ;;   our (filter #(not (drop-set %)).
  (drop-cols-by-index
   [this cols]
   (if (= (ncol this) 0)
     this
     (do
       (assert (sequential? cols)
               "drop-cols-by-index: column indices must be supplied in a sequential collection.")
       (assert (every? integer? cols)
               "drop-cols-by-index: supplied column indices must be integers.")
       (assert (every? #(not (neg? %)) cols)
               "drop-cols-by-index: negative column indices not allowed.")
       
       (let [drop-set (set cols)]
         (select-cols-by-index this
                               (util/vfilter #(not (drop-set %))
                                             (util/vrange column-ct)))))))
  (drop-rows-by-index
   [this rows]
   (if (= (nrow this) 0)
     this
     (do
       (assert (sequential? rows)
               "drop-rows-by-index: row indices must be supplied in a sequential collection.")
       (assert (every? integer? rows)
               "drop-rows-by-index: supplied row indices must be integers.")
       (assert (every? #(not (neg? %)) rows)
               "drop-rows-by-index: negative row indices not allowed.")
       
       (let [drop-set (set rows)]
         (select-rows-by-index this
                               (util/vfilter #(not (drop-set %))
                                             (util/vrange row-ct)))))))

  ($-
   [this col-spec]
   ($- this col-spec nil))

  ($-
   [this col-spec row-spec]
   ;; if either or both of the specifiers are nil,
   ;;   don't apply filter to that axis.
   (let [filtered this
         filtered (if (nil? col-spec)
                    filtered
                    (let [col-indices (interpret-spec this :cols col-spec)]
                      (drop-cols-by-index filtered col-indices)))
         filtered (if (nil? row-spec)
                    filtered
                    (let [row-indices (interpret-spec this :rows row-spec)]
                      (drop-rows-by-index filtered row-indices)))]
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
     (replace-col this colname column)
     (add-col this colname column)))

  (set-col
   [this set-map]
   (assert (associative? set-map)
           (str "set-col: set-map must be associative. (Received " (type set-map) ".)"))
   (reduce (fn [df [colname column]]
             (set-col df colname column))
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
   (let [left-colnames (explojure.dataframe.core/colnames this)
         right-colnames (explojure.dataframe.core/colnames right)
         left-rownames (explojure.dataframe.core/rownames this)
         right-rownames (explojure.dataframe.core/rownames right)
         
         colname-conflicts (s/intersection (set left-colnames)
                                           (set right-colnames))
         equal-nrow (= (nrow this)
                       (nrow right))

         left-has-rownames (not (nil? left-rownames))
         right-has-rownames (not (nil? right-rownames))
         
         align-rownames (and left-has-rownames
                             right-has-rownames)]
     
     (assert (= (count colname-conflicts) 0)
             (str "conj-cols: colnames must be free of conflicts (" colname-conflicts "). Consider using (merge-frames) if appropriate."))
     
     (if align-rownames
       ;; if both DFs have rownames
       (let [[left-only in-common left-and-common right-only] (venn-components left-rownames
                                                                               right-rownames)
             
             combined-colnames
             (util/vconcat left-colnames right-colnames)

             combined-columns
             (cmb-cols-vt (cmb-cols-hr (col-vectors ($ this nil left-only))
                                       (nil-cols (ncol right) (count left-only)))
                          (cmb-cols-hr (col-vectors ($ this nil in-common))
                                       (col-vectors ($ right nil in-common)))
                          (cmb-cols-hr (nil-cols (ncol this) (count right-only))
                                       (col-vectors ($ right nil right-only))))
             
             combined-rownames
             (util/vconcat left-only in-common right-only)]
         
         ($ (new-dataframe combined-colnames
                           combined-columns
                           combined-rownames)
            (concat left-and-common right-only)
            nil))

       ;; if one or none of the DFs have rownames
       (let [combined-colnames (util/vconcat left-colnames
                                             right-colnames)

             combined-columns (util/vconcat (col-vectors this)
                                            (col-vectors right))

             ;; at this point we know it is not true that both DFs have rownames
             ;; so use the one that has them.  Or nil if neither has them.
             use-rownames (if (explojure.dataframe.core/rownames this)
                            (explojure.dataframe.core/rownames this)
                            (explojure.dataframe.core/rownames right))

             [left-only in-common left-and-common right-only] (venn-components left-colnames
                                                                               right-colnames)]
         ($ (new-dataframe combined-colnames
                           combined-columns
                           use-rownames)
            (concat left-and-common right-only)
            nil)))))

  (conj-rows
   [this bottom]
   (let [top-rownames (explojure.dataframe.core/rownames this)
         bottom-rownames (explojure.dataframe.core/rownames bottom)

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

       (let [top-colnames (explojure.dataframe.core/colnames this)
             bottom-colnames (explojure.dataframe.core/colnames bottom)

             [top-only in-common top-and-common bottom-only] (venn-components top-colnames
                                                                              bottom-colnames)
             cmb-colnames (util/vconcat top-only in-common bottom-only)
             
             cmb-columns (cmb-cols-vt
                          (cmb-cols-hr (col-vectors ($ this top-only nil))
                                           (col-vectors ($ this in-common nil))
                                           (nil-cols (count bottom-only)
                                                     (nrow this)))
                          (cmb-cols-hr (nil-cols (count top-only)
                                                     (nrow bottom))
                                           (col-vectors ($ bottom in-common nil))
                                           (col-vectors ($ bottom bottom-only nil))))
             
             cmb-rownames (if both-have-rownames
                            (util/vconcat top-rownames
                                          bottom-rownames)
                            nil)]
         
         ($ (new-dataframe cmb-colnames
                           cmb-columns
                           cmb-rownames)
            (concat top-and-common bottom-only)
            nil))))

  (merge-frames
   [this right resolution-fn]
   (let [left-colnames (explojure.dataframe.core/colnames this)
         right-colnames (explojure.dataframe.core/colnames right)
         left-rownames (explojure.dataframe.core/rownames this)
         right-rownames (explojure.dataframe.core/rownames right)]

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
           (venn-components left-rownames right-rownames)
           [cn-left-only cn-in-common cn-left-and-common cn-right-only]
           (venn-components left-colnames right-colnames)

           ;; overlap / conflict
           conflict-columns (for [c cn-in-common]
                              [c rn-in-common])

           ;; resolve conflicts
           resolved (vec (for [[col rownames] conflict-columns]
                           (util/vmap #(apply resolution-fn %)
                                      (map vector
                                           (repeat col)
                                           rownames
                                           ($ this col rownames)
                                           ($ right col rownames)))))

           combined-columns
           (cmb-cols-vt
            (cmb-cols-hr (col-vectors ($ this cn-left-only rn-left-only))
                         (col-vectors ($ this cn-in-common rn-left-only))
                         (nil-cols (count cn-right-only) (count rn-left-only)))
            
            (cmb-cols-hr (col-vectors ($ this cn-left-only rn-in-common))
                         resolved
                         (col-vectors ($ right cn-right-only rn-in-common)))
            
            (cmb-cols-hr (nil-cols (count cn-left-only) (count rn-right-only))
                         (col-vectors ($ right cn-in-common rn-right-only))
                         (col-vectors ($ right cn-right-only rn-right-only))))
           
           combined-colnames (util/vconcat cn-left-only cn-in-common cn-right-only)
           
           combined-rownames (util/vconcat rn-left-only rn-in-common rn-right-only)]
       
       ($ (new-dataframe combined-colnames
                         combined-columns
                         combined-rownames)
          (concat cn-left-and-common cn-right-only)
          (concat rn-left-and-common rn-right-only)))))

  (join-frames
   [this right on join-type]
   (join-frames this right on on join-type))

  (join-frames
   [this right left-on right-on join-type]
   (join/join this right left-on right-on join-type)))

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

(defn interpret-nil-spec [df axis nl] nil)

(defn interpret-empty-spec [df axis mpt] [])

(defn interpret-fn-spec [df axis f]
  (when (not= (count f) 1)
    (throw (new Exception "Only one function can be passed per specifier.")))
  (let [f (first f)]
    (interpret-spec df
                    axis
                    (map boolean
                         (map f (case axis
                                  :cols (col-vectors df)
                                  :rows (row-vectors df)))))))

(defn interpret-int-spec [df axis ints]
  (let [n (lookup-names df axis ints)]
    (if (util/no-nil? n)
      n
      ints)))

(defn interpret-bool-spec [df axis bools]
  (when (not= (count bools) (case axis
                              :cols (ncol df)
                              :rows (nrow df)))
    (throw (new Exception "Boolean specifiers must have length equal to ncol or nrow.")))
  (util/where bools))

(defn interpret-name-spec [df axis names]
  (let [n (lookup-names df axis names)]
    (if (util/no-nil? n)
      n
      (throw
       (new Exception
            (str "The given DataFrame does not have the following "
                 (case axis
                   :cols "column"
                   :rows "row")
                 " names: "
                 (vec (util/filter-ab (map not (map boolean n))
                                      names))))))))

;; TODO: This conditional code looks brittle to me.
;;   Think about if there is a better way to do this.
(defn interpret-spec [df axis spec]
  (cond
    ;; handle nils
    (nil? spec)
    (interpret-nil-spec df axis spec)
    
    ;; handle empty sequentials
    (and (sequential? spec)
         (= (count spec) 0))
    (interpret-empty-spec df axis spec)
    
    ;; handle everything else
    :else
    (let [spec (util/make-vector spec)
          f (first spec)]
      (when (not (util/same-type spec))
        (throw
         (new Exception "All elements of spec must be of the same type.")))
      (cond
        (fn? f)                       (interpret-fn-spec df axis spec)
        (integer? f)                  (interpret-int-spec df axis spec)
        (util/boolean? f)             (interpret-bool-spec df axis spec)
        (or (string? f) (keyword? f)) (interpret-name-spec df axis spec)
        :else (throw (new Exception (str "Specifiers must be nil, [], fn, integer, boolean, string, or keyword. Received " (type f))))))))



