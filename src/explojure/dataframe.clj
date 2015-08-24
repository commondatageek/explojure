(ns explojure.dataframe
  (:require [explojure.util :as util]))


(defn vconcat [& args] (vec (apply concat args)))
(defn vconj [& args] (vec (apply conj args)))
(defn vmap [& args] (vec (apply map args)))
(defn vrepeat [& args] (vec (apply repeat args)))

(defn unwoven-array-map
  [keys values]
  (apply array-map (interleave keys values)))

(defn unique [xs]
  (first (reduce (fn [a b]
                      (let [[ord unq] a]
                        (if (contains? unq b)
                          [ord unq]
                          [(conj ord b) (conj unq b)])))
                    [[] (hash-set)]
                    xs)))

(defn rename-key [m old-key new-key]
  (if (or (not (contains? m old-key))
          (= old-key new-key))
    m
    (dissoc (assoc m new-key (get m old-key))
            old-key)))

(defn rename-keys [m rename]
  (reduce (fn [updated repl-pair]
            (let [[old-key new-key] repl-pair]
              (rename-key updated old-key new-key)))
          m
          (seq rename)))


(defn $df
  "Placeholder to allow compilation."
  [& keyvals])

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
  ($ [this cols rows]
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

  (invoke [this cols rows]
          ($ this cols rows))
  
  ($col [this col]
        (if (sequential? col)
          ;; if > 1 column names passed, return DataFrame
          ($ this col nil)
          ;; if 1 column name passed, return vector
          (data-hash col)))
  
  ($row [this row]
        ($ this nil (if (sequential? row)
                      row
                      [row])))

  ($rows [this]
         (let [rcr-fn (fn row-fn [ordered-lazy]
                        (if (seq (first ordered-lazy))
                          (cons (vec (for [c ordered-lazy] (first c)))
                                (lazy-seq (row-fn (vec (for [c ordered-lazy] (rest c))))))))]
           (rcr-fn (for [c (vmap data-hash columns)]
                     (lazy-seq c)))))
  
  ($ [this cols rows]
     (let [cols (if (nil? cols)
                  ($colnames this)
                  cols)]
       (apply $df
              (interleave cols
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
        (new DataFrame
             (vconj columns dst-col)
             row-count
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
                     col-data)]
      (new DataFrame
           (if (contains? data-hash col-name)
             columns
             (vconj columns col-name))
           row-count
           (assoc data-hash col-name col-data))))
  
  ($count [this]
    (vmap (fn [col]
            (count (filter (fn [x] (not (nil? x)))
                           ($col this col))))
          ($colnames this)))

  ($conj-rows [this d2]
     (if (nil? d2)
      this
      (let [this-cols ($colnames this)
            d2-cols ($colnames d2)
            unique-cols (unique (vconcat this-cols d2-cols))]
        (apply $df
               (interleave unique-cols
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
                (new DataFrame
                     (replace repl-map columns)
                     row-count
                     (rename-keys data-hash repl-map)))

  ($remove-cols [this cols]
    (new DataFrame
         (remove #(contains? (set cols) %)
                 columns)
         row-count
         (reduce (fn [m c] (dissoc data-hash c))
                 data-hash
                 cols)))

  ($replace-cols [this repl-map]
    (new DataFrame
         columns
         row-count
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
  (let [idx (range (count keyvals))
        columns (vmap (vec keyvals)
                      (filter even? idx))
        data-hash (apply hash-map keyvals)
        row-count (count (second (first data-hash)))]
    (new DataFrame
         columns
         row-count
         data-hash)))
