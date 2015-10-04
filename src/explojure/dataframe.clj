(ns explojure.dataframe
  (:require [explojure.util :as util]
            [clojure.set :as s]
            [clojure.test :as t]))

(defmacro vectorize [lazy-fns]
  (concat '(do)
          (for [f lazy-fns]
            `(def ~(symbol (str "v" f))
               (fn [& ~'args]
                 (vec (apply ~f ~'args)))))))

(vectorize [concat conj dedupe distinct doall drop drop-last drop-while
            filter interleave interpose keep keys map partition pmap
            range remove repeat reverse take take-nth take-while vals])

;; Placeholder to allow compilation.
(declare new-dataframe)

(defprotocol Tabular
  "The Tabular protocol represents tabular data.  (Rows and columns)."

  ($col [this col] "Return the specified column(s)")
  ($row [this row] "Return the specified row(s)")
  ($rows [this] "Return a lazy sequence of row vectors ")
  ($ [this rows cols] "Select the specified columns and rows from this")
  ($nrow [this] "Return the number of rows")
  ($ncol [this] "Return the number of columns")
  ($colnames [this] "Return the column names")
  ($map [this src-col f]
        [this src-col f dst-col]
        "Return the result of (map)-ing src-col by function f.  If dst-col is given, return this Tabular with a new column.")
  ($count [this] "Show the number of non-nil values in each column")
  ($describe [this])  
  ($add-col [this col-name col-data] "Add a new column. Fails if column already exists.")
  ($replace-col [this col-name col-data] "Replace an existing column. Fails if column does not exist.")
  ($set-col [this name col-data] "Set new values for this column")
  ($conj-rows [this d2] "Join two Tabulars along the row axis")
  ($conj-cols [this d2] "Join two Tabulars along the column axis")
  ($rename-cols [this repl-map])
  ($remove-cols [this cols])
  ($replace-cols [this repl-map])
  ($raw [this] "Return the raw data structures underlying this DataFrame")
  ($xf [this f from-col] [this f from-col to-col])
  ($xfc [this f from-col] [this f from-col to-col]))

(defn all-equal?
  "Return true if all xs are equal. For large xs, this fail-fast
implementation may be more efficient than applying all xs as parameters
to apply."
  [xs]
  (let [x1 (first xs)
        x2 (fnext xs)]
      (if (or (nil? x1)
              (nil? x2))
        true
        (if (not= x1 x2)
          false
          (recur (next xs))))))

(defn unique? [xs]
  (= (count xs)
     (count (vdistinct xs))))

(defn ensure-type
  ([x t]
   (ensure-type x t "Argument x must have type t."))

  ([x t err-msg]
   (when (not= (type x) t)
     (throw (new Exception err-msg)))
   true))

(defn ensure-vvector
  ([xs]
   (ensure-vvectors xs "Argument xs must be a vector of vectors."))

  ([xs err-msg]
   (ensure-vector xs err-msg)
   (doseq [x xs]
     (ensure-vector x err-msg))
   true))

(defn ensure-equal
  ([a b]
   (ensure-equal a b "Arugments a and b must be equal"))

  ([a b err-msg]
   (when (not= a b)
     (throw (new Exception err-msg)))
   true))

(defn ensure-all-equal
  ([xs]
   (ensure-all-equal xs "All elements of xs must be equal."))

  ([xs err-msg]
   (when (not (all-equal? xs))
     (throw (new Exception err-msg)))
   true))

(defn ensure-membership
  ([x s]
   (ensure-membership x s "Argument x must be a member of set s."))

  ([x s err-msg]
   (when (not (contains? s x))
     (throw (new Exception err-msg)))
   true))

(defn ensure-unique
  ([xs]
   (ensure-unique xs "Elements of xs must be unique."))

  ([xs err-msg]
   (when (not (unique? xs))
     (throw (new Exception err-msg)))
   true))

(defn validate-columns [colnames columns]
  ;; colnames must be a vector
  (ensure-type colnames
               clojure.lang.PersistentVector
               "colnames argument must be a vector")

  ;; colnames must all be unique
  (ensure-unique colnames
                 "colnames must be unique.")
  
  ;; columns must be a vector of vectors
  (ensure-vvector columns
                  "columns argument must be a vector of vectors")

  ;; column vectors must have the same length
  (ensure-all-equal (map count columns)
                    "All columns must have the same length.")
  
  ;; colnames must have same length as columns
  (ensure-equal (count colnames)
                (count columns)
                "There must be a 1:1 correspondence of columns to column names.")

  ;; all colnames must be the same type
  (ensure-all-equal (map type colnames)
                    "All colnames must have the same type.")

  ;; colname type must be String, keyword, or integer
  (when (> (count colnames) 0)
    (ensure-membership (type (first colnames))
                       #{;; strings
                         java.lang.String
                         ;; keywords
                         clojure.lang.Keyword
                         ;; integers
                         clojure.lang.BigInt
                         java.lang.Short
                         java.lang.Integer
                         java.lang.Long}
                       "All colnames must be of type string, keyword, or integer"))
  

  true)

(defn validate-rownames [rownames row-ct]
  ;; rownames must be a vector
  (ensure-type rownames
               clojure.lang.PersistentVector
               "rownames argument must be a vector")

  ;; rownames must equal row-ct
  (ensure-equal (count rownames)
                row-ct
                "rownames argument must have same length as columns")

  ;; rownames must all be unique
  (ensure-unique rownames
                 "rownames must be unique.")
  
  ;; all rownames must be the same type
  (ensure-all-equal (map type rownames)
                    "All rownames must have the same type.")

  ;; rowname type must be String, keyword, or integer
  (when (> (count rownames) 0)
    (ensure-membership (type (first rownames))
                       #{;; strings
                         java.lang.String
                         ;; keywords
                         clojure.lang.Keyword
                         ;; integers
                         clojure.lang.BigInt
                         java.lang.Short
                         java.lang.Integer
                         java.lang.Long}
                       "All rownames must be of type string, keyword, or integer"))
  

  true)

(deftype DataFrame
    [colnames    ; a vector of column names, giving column order
     columns     ; a vector of column vectors
     column-ct   ; the number of columns
     colname-idx ; a hash-map of colname => 0-based index
     row-ct      ; the number of rows
     rowname-idx ; (optional) a hash-map of rowname => 0-based index
     ])

(defn new-dataframe
  ([colnames columns]
   ;; ensure data assumptions
   (validate-columns colnames columns)

   ;; create a new DataFrame
   (let [column-ct (count colnames)
         colname-idx (reduce (fn [m [k i]]
                               (assoc m k i))
                             {}
                             (map vector
                                  colnames
                                  (range column-ct)))
         row-ct (count (first columns))]
     (->DataFrame colnames
                  columns
                  column-ct
                  colname-idx
                  row-ct
                  nil)))


  
  ([colnames columns rownames]
   ;; ensure column assumptions
   (validate-columns colnames columns)
   (let [column-ct (count colnames)
         colname-idx (reduce (fn [m [k i]]
                               (assoc m k i))
                             {}
                             (map vector
                                  colnames
                                  (range column-ct)))
         row-ct (count (first columns))]

     ;; ensure rowname assumptions
     (validate-rownames rownames row-ct)
     (let [rowname-idx (reduce (fn [m [k i]]
                                 (assoc m k i))
                               {}
                               (map vector
                                    rownames
                                    (range row-ct)))]
       ;; create a new DataFrame
       (->DataFrame colnames
                    columns
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
  (let [idx (vrange (count keyvals))
        colnames (vmap (vec keyvals)
                       (vfilter even? idx))
        columns (vmap (vec keyvals)
                      (vfilter odd? idx))]
    (new-dataframe colnames
                   columns)))

