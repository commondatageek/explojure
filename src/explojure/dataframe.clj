(ns explojure.dataframe)

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
  ($add-col [this col name]
            "Add a column to the dataframe")
  ($set-col [this name col-data]
            "Set new values for this column")
  ($conj-rows [this d2]
              "Join two Tabulars along the row axis")
  ($conj-cols [this d2]
              "Join two Tabulars along the column axis")
  ($rename-cols [this repl-map])
  ($remove-cols [this cols])
  ($raw [this]
        "Return the raw data structures underlying this DataFrame"))

(deftype DataFrame [columns data-hash]
  ;; A data table.  The core data structure is an array-map where
  ;; the keys are column names and the values are vectors of
  ;; column data.
  
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
           (rcr-fn (for [c (map data-hash columns)]
                     (lazy-seq c)))))
  
  ($ [this cols rows]
     (let [cols (if (nil? cols)
                  ($colnames this)
                  cols)]
       (apply $df
              (interleave cols
                          (mapv (fn [col] (if (nil? rows)
                                            ($col this col)
                                            (mapv ($col this col) rows)))
                                cols)))))
  
  ($nrow [this]
         (count (data-hash (first (keys data-hash)))))
  
  ($ncol [this]
         (count columns))
  
  ($colnames [this]
             columns)
  
  ($map [this src-col f]
    (mapv f (data-hash src-col)))
  ($map [this src-col f dst-col]
        (new DataFrame
             (conj columns dst-col)
             (assoc data-hash
                    dst-col
                    (mapv f (data-hash src-col)))))
  
  ($describe [this]
             (map #(println (str % ": "
                                 (type ($col this %))))
                  ($colnames this)))
  
  ($add-col [this col name]
            (new DataFrame
                 (conj columns name)
                 (assoc data-hash name col)))
  ($set-col [this name data-col]
            (new DataFrame
                 columns
                 (assoc data-hash name data-col)))

  ($count [this]
    (mapv (fn [col]
            (count (filter (fn [x] (not (nil? x)))
                           ($col this col))))
          ($colnames this)))

  ($conj-rows [this d2]
     (if (nil? d2)
      this
      (let [this-cols ($colnames this)
            d2-cols ($colnames d2)
            unique-cols (unique (concat this-cols d2-cols))]
        (apply $df
               (interleave unique-cols
                           (map (fn [x]
                                  (let [this-vals ($col this x)
                                        d2-vals ($col d2 x)]
                                    (concat (if (nil? this-vals)
                                              (repeat ($nrow d2) nil)
                                              this-vals)
                                            (if (nil? d2-vals)
                                              (repeat ($nrow this) nil)
                                              d2-vals))))
                                unique-cols))))))

  ($conj-cols [this d2]
    (if (nil? d2)
      this
      (let [d2-cols ($colnames d2)]
        (reduce (fn [new-df col]
                  (let [raw ($col d2 col)]
                    ($add-col new-df
                              raw
                              col)))
                this
                ($colnames d2)))))

  ($rename-cols [this repl-map]
                (new DataFrame
                     (replace repl-map columns)
                     (rename-keys data-hash repl-map)))

  ($remove-cols [this cols]
    (new DataFrame
         (remove #(contains? (set cols) %)
                 columns)
         (reduce (fn [m c] (dissoc data-hash c))
                 data-hash
                 cols)))

  ($raw [this]
        [columns data-hash])
  
  

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
        columns (mapv (vec keyvals)
                     (filter even? idx))]
    (new DataFrame
         columns
         (apply hash-map keyvals))))

