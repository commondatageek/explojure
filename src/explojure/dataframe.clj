(ns explojure.dataframe
  (:require [explojure.util :as util]
            [clojure.set :as s]))

(defmacro vectorize [lazy-fns]
  (concat '(do)
          (for [f lazy-fns]
            `(def ~(symbol (str "v" f))
               (fn [& ~'args]
                 (vec (apply ~f ~'args)))))))

(vectorize [concat conj dedupe distinct doall drop drop-last drop-while
            filter interleave interpose keep keys map partition pmap
            range remove repeat reverse take take-nth take-while vals])

(defn rename-key
  "In map m, rename old-key to new-key."
  [m old-key new-key]
  (if (or (not (contains? m old-key))
          (= old-key new-key))
    m
    (dissoc (assoc m new-key (get m old-key))
            old-key)))

(defn rename-keys
  "In map m, perform multiple key renames."
  [m rename]
  (reduce (fn [updated repl-pair]
            (let [[old-key new-key] repl-pair]
              (rename-key updated old-key new-key)))
          m
          (seq rename)))


;; Placeholder to allow compilation.
(declare $df)
(declare new-dataframe)

(defprotocol Tabular
  "
  The Tabular protocol represents tabular data.  (Rows and columns).
  "
  ($col [this col]
    "Return the specified column(s)")
  ($row [this row]
        "Return the specified row(s)")
  ($rows [this]
         "Return a lazy sequence of row vectors ")
  ($ [this rows cols]
    "Select the specified columns and rows from this")
  ($nrow [this]
    "Return the number of rows")
  ($ncol [this]
    "Return the number of columns")
  ($colnames [this]
    "Return the column names")
  ($map [this src-col f] [this src-col f dst-col]
    "Return the result of (map)-ing src-col by function f.  If dst-col is
  given, return this Tabular with a new column.")
  ($count [this]
    "Show the number of non-nil values in each column")
  ($describe [this])
  ($set-col [this name col-data]
            "Set new values for this column")
  ($conj-rows [this d2]
              "Join two Tabulars along the row axis")
  ($conj-cols [this d2]
              "Join two Tabulars along the column axis")
  ($rename-cols [this repl-map])
  ($remove-cols [this cols])
  ($replace-cols [this repl-map])
  ($raw [this]
        "Return the raw data structures underlying this DataFrame")
  ($xf [this f from-col] [this f from-col to-col])
  ($xfc [this f from-col] [this f from-col to-col]))

(deftype DataFrame
    [columns   ; a vector keeping the order of the column names
     row-count ; an integer giving the number of rows
     data-hash ; a hash-map containing the column vectors
     ]
  
  Tabular
  clojure.lang.IFn

  (invoke [this rows cols]
          ($ this rows cols))
  
  ($col [this col]
        (if (sequential? col)
          ;; if > 1 column names passed, return DataFrame
          ($ this nil col)
          ;; if 1 column name passed, return vector
          (data-hash col)))
  
  ($row [this row]
        ($ this
           (if (sequential? row)
             row
             [row])
           nil))

  ($rows [this]
         (let [rcr-fn (fn row-fn [ordered-lazy]
                        (if (seq (first ordered-lazy))
                          (cons (vec (for [c ordered-lazy] (first c)))
                                (lazy-seq (row-fn (vec (for [c ordered-lazy] (rest c))))))))]
           (rcr-fn (for [c (vmap data-hash columns)]
                     (lazy-seq c)))))
  
  ($ [this rows cols]
     (let [cols (if (nil? cols)
                  ($colnames this)
                  cols)]
       (apply $df
              (vinterleave cols
                          (vmap (fn [col]
                                  (if (nil? rows)
                                    ($col this col)
                                    (vmap ($col this col) rows)))
                                cols)))))
  
  ($nrow [this] row-count)
  
  ($ncol [this]
         (count columns))
  
  ($colnames [this]
             columns)
  
  ($map [this src-col f]
    (vmap f (data-hash src-col)))
  ($map [this src-col f dst-col]
        (new-dataframe
             (vconj columns dst-col)
             (assoc data-hash
                    dst-col
                    (vmap f (data-hash src-col)))))
  
  ($describe [this]
             (vmap #(println (str % ": "
                                 (type ($col this %))))
                  ($colnames this)))
  
  ($set-col [this col-name col-data]
    (let [col-data (if (not (coll? col-data))
                     (vrepeat ($nrow this) col-data)
                     (vec col-data))]
      (new-dataframe
           (if (contains? data-hash col-name)
             columns
             (vconj columns col-name))
           (assoc data-hash col-name col-data))))
  
  ($count [this]
    (vmap (fn [col]
            (count (vfilter (fn [x] (not (nil? x)))
                           ($col this col))))
          ($colnames this)))

  ($conj-rows [this d2]
     (if (nil? d2)
      this
      (let [this-cols ($colnames this)
            d2-cols ($colnames d2)
            unique-cols (vdistinct (vconcat this-cols d2-cols))]
        (apply $df
               (vinterleave unique-cols
                           (vmap (fn [x]
                                  (let [this-vals ($col this x)
                                        d2-vals ($col d2 x)]
                                    (vconcat (if (nil? this-vals)
                                              (vrepeat ($nrow d2) nil)
                                              this-vals)
                                            (if (nil? d2-vals)
                                              (vrepeat ($nrow this) nil)
                                              d2-vals))))
                                unique-cols))))))

  ($conj-cols [this d2]
    (if (nil? d2)
      this
      (let [d2-cols ($colnames d2)]
        (reduce (fn [new-df col]
                  (let [raw ($col d2 col)]
                    ($set-col new-df
                              col
                              raw)))
                this
                ($colnames d2)))))

  ($rename-cols [this repl-map]
                (new-dataframe
                     (replace repl-map columns)
                     (rename-keys data-hash repl-map)))

  ($remove-cols [this cols]
    (new-dataframe
         (vremove #(contains? (set cols) %)
                 columns)
         (reduce (fn [m c] (dissoc data-hash c))
                 data-hash
                 cols)))

  ($replace-cols [this repl-map]
    (new-dataframe
         columns
         (reduce (fn [m [key val]]
                   (if (contains? m key)
                     (assoc m key val)
                     m))
                 data-hash
                 (seq repl-map))))

  ($raw [this]
        [columns data-hash])

  ($xf [this f from-col]
       ($xf this f from-col from-col))

  ($xf [this f from-col to-col]
       (if (= (count from-col) 1)
         ;; one column, value by value
         ($set-col this
                   (first to-col)
                   (vmap f ($col this (first from-col))))
         ;; multiple columns, value by value
         (let [col-subset ($col this from-col)]
           (reduce (fn [old-df [colname data]]
                     ($set-col old-df colname data))
                   this
                   (vmap vector
                        to-col
                        (util/rows->cols (vmap f ($rows col-subset))))))))

  ($xfc [this f from-col]
        ($xfc this f from-col from-col))

  ($xfc [this f from-col to-col]
        (if (= (count from-col) 1)
          ($set-col this
                    (first to-col)
                    (f ($col this (first from-col))))
          (reduce (fn [old-df [colname data]]
                    ($set-col old-df colname data))
                  this
                  (vmap vector
                       to-col
                       (f (vmap #($col this %) from-col))))))
  
  
  

  Object
  (toString [this] (str "<DataFrame: " ($nrow this) "R, " ($ncol this) "C>")))


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
        columns (vmap (vec keyvals)
                      (vfilter even? idx))
        data-hash (apply hash-map keyvals)]
    (new-dataframe
         columns
         data-hash)))



;; ways to create dataframes
;; 1. hash-map: column name => column data
;; 2. hash-map + column vector
;; 3. alternating scalar/vector arguments
;; 4.

(defn set-equal [s1 s2]
  (and
   (empty? (s/difference s1 s2))
   (empty? (s/difference s2 s1))))

(defn equal-count [& colls]
  (apply = (for [c colls]
             (count c))))

(defn vectorize-vals
  "Make certain that all vals in the map m are vectors."
  [m]
  (reduce (fn [old-m k]
            (assoc old-m k (vec (get old-m k))))
          m
          (keys m)))

(defn new-dataframe
  "Create a new DataFrame.

  Arguments
  - columns: a vector of column names, giving the column order
  - data-map: a hash- or array-map mapping column names to data vectors.
  
  Must be 1:1 mapping of values in columns to keys in data-map.
  All data vectors must be the same length."
  [columns data-map]
  ;; basic checks for data integrity
  (when-not (set-equal (set columns) (set (vkeys data-map)))
    (throw (new Exception "columns must have same values as data-map keys")))
  (when-not (apply equal-count (vvals data-map))
    (throw (new Exception "data-map values must be vectors of equal length")))

  ;; create the DataFrame object
  (let [nrow (count (first (vvals data-map)))]
    (new DataFrame
         (vec columns)
         nrow
         (vectorize-vals data-map))))
