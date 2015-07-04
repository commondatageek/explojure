(ns explojure.core)

(defprotocol Tabular
  "
  The Tabular protocol represents tabular data.  (Rows and columns).
  "
  ($col [this col]
    "Return the specified column(s)")
  ($row [this row]
    "Return the specified row(s)")
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
  ($describe [this]))

(deftype DataFrame [data-array-map]
  ;; A data table.  The core data structure is an array-map where
  ;; the keys are column names and the values are vectors of
  ;; column data.
  
  Tabular
  
  ($col [this col] (if (sequential? col)
                     ;; if > 1 column names passed, return DataFrame
                     ($ this col nil)
                     ;; if 1 column name passed, return vector
                     (data-array-map col)))
  
  ($row [this row] ($ this nil (if (sequential? row)
                                 row
                                 [row])))
  
  ($ [this cols rows]
    (let [cols (if (nil? cols)
                 ($colnames this)
                 cols)]
      (new DataFrame
           (apply array-map
                  (interleave cols
                              (mapv (fn [col] (if (nil? rows)
                                                ($col this col)
                                                (mapv ($col this col) rows)))
                                    cols))))))
  
  ($nrow [this]
    (count (data-array-map (first (keys data-array-map)))))
  
  ($ncol [this]
    (count (keys data-array-map)))
  
  ($colnames [this]
    (keys data-array-map))
  
  ($map [this src-col f]
    (mapv f (data-array-map src-col)))
  ($map [this src-col f dst-col]
    (new DataFrame (assoc data-array-map
                          dst-col
                          (mapv f ($col this src-col)))))
  ($describe [this]
    (println (type data-array-map))
    (map #(println (type ($col this %))) ($colnames this)))

  ($count [this]
    (mapv (fn [col]
            (count (filter (fn [x] (not (nil? x)))
                           ($col this col))))
          ($colnames this)))
  
  Object
  (toString [this] (clojure.pprint/pprint data-array-map)))

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
  (loop [d (array-map)
         remaining (reverse keyvals)]
    (if (seq remaining)
      (let [current (reverse (take 2 remaining))
            column (first current)
            data (second current)]
        (recur (assoc d column (vec data)) (drop 2 remaining)))
      (new DataFrame d))))


