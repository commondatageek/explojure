(ns explojure.dataframe.core
     (:require [explojure.dataframe.validation :as valid]
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

(defprotocol Tabular
  "The Tabular protocol represents tabular data.  (Rows and columns)."

  (colnames [this])
  (rownames [this])
  (ncol [this])
  (nrow [this])
  (not-nil [this])

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
      [this col-spec row-spec]))

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

  ;; convert names to 0-based indices
  (lookup-names
   [this axis xs]
   (let [index (case axis
                 :rows rowname-idx
                 :cols colname-idx
                 (throw (new Exception "axis must be :rows or :cols")))]
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
   (new-dataframe (util/vmap colnames cols)
                  (util/vmap columns cols)))
  (select-rows-by-index
   [this rows]
   (new-dataframe colnames
                  (apply vector
                         (for [c columns]
                           (util/vmap c rows)))))
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
  (drop-cols-by-index
   [this cols]
   (let [drop-set (set cols)]
     (select-cols-by-index this
                           (util/vfilter #(not (drop-set %))
                                         (util/vrange column-ct)))))
  (drop-rows-by-index
   [this rows]
   (let [drop-set (set rows)]
     (select-rows-by-index this
                           (util/vfilter #(not (drop-set %))
                                         (util/vrange row-ct)))))

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
     filtered)))

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
                  nil
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


