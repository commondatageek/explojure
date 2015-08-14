(ns explojure.join
  (:require [explojure.dataframe :as df]
            [clojure.set :as set]))

(defn gen-index [& cols]
  (reduce (fn [index entry]
            (let [[key val] entry]
              (assoc index
                     key
                     (conj (get index key [])
                           val))))
          (hash-map)
          (map vector
               (apply (partial map vector) cols)
               (range (count (first cols))))))


(defn- filter-keys [l-idx r-idx join-type]
  (let [l-keys (apply hash-set (keys l-idx))
        r-keys (apply hash-set (keys r-idx))
        ;; all join types will have the inner keys
        filtered-keys (set/intersection l-keys r-keys)
        
        ;; if :left or :outer, add the left-only keys
        filtered-keys (if (or (= join-type :left)
                              (= join-type :outer))
                        (set/union filtered-keys
                                   (set/difference l-keys r-keys))
                        filtered-keys)

        ;; if :right or :outer, add the right-only keys
        filtered-keys (if (or (= join-type :right)
                              (= join-type :outer))
                        (set/union filtered-keys
                                   (set/difference r-keys l-keys))
                        filtered-keys)]
    filtered-keys))

(defn- choose-rows [l-idx r-idx filtered-keys]
  (reduce  (fn [[left-only [left-inner right-inner] right-only] key]
             (let [l-rows (l-idx key)
                   r-rows (r-idx key)]
               [;; handle left-only
                (if (nil? r-rows)
                  (concat left-only l-rows)
                  left-only)

                ;; handle inner
                (if (and l-rows r-rows)
                  [(concat left-inner
                           (sort (apply concat (repeat (count r-rows) l-rows))))
                   (concat right-inner
                           (apply concat (repeat (count l-rows) (sort r-rows))))]
                  [left-inner right-inner])

                ;; handle right-only
                (if (nil? l-rows)
                  (concat right-only r-rows)
                  right-only)]))

           ;; [left-only [left-inner right-inner] right-only]
           [[] [[] []] []]
           
           filtered-keys))

(defn empty-df [colnames nrows]
  (let [empty-col (vec (repeat nrows nil))]
    (apply df/$df (interleave colnames (repeat empty-col)))))

(defn avoid-collisions [l-join r-join l r]
  (let [l-non-join (set/difference (set l) (set l-join))
        r-non-join (set/difference (set r) (set r-join))
        collisions (set/intersection l-non-join r-non-join)
        repl-fn (fn [suff]
                  (reduce (fn [repl-map col]
                            (assoc repl-map
                                   col
                                   (if (keyword? col)
                                     (keyword (str (name col) suff))
                                     (str col suff))))
                          (hash-map)
                          collisions))]
    [(repl-fn "_x")
     (repl-fn "_y")]))

(defn join
  ([left right on join-type] (join left right on on join-type))
  ([left right left-on right-on join-type]
   (let [;; create index for key columns on left and right DFs
         l-idx (apply gen-index (for [c left-on] (df/$col left c)))
         r-idx (apply gen-index (for [c right-on] (df/$col right c)))

         ;; choose the row indices to draw from each DF
         [left-only
          [left-inner right-inner]
          right-only] (choose-rows l-idx
                                   r-idx
                                   (filter-keys l-idx r-idx join-type))

         ;; column names in the DFs
         left-cols (df/$colnames left)
         right-cols (df/$colnames right)

         ;; intersect join columns
         common-join-cols (set/intersection (set left-on) (set right-on))

         ;; get the appropriate rows from each DF
         ri-rows (df/$ right right-cols right-inner)
         re-rows (empty-df right-cols (count left-only))
         ro-rows (df/$ right right-cols right-only)

         li-rows (df/$ left left-cols left-inner)
         lo-rows (df/$ left left-cols left-only)

         ;; left-empty frame is special because we need to
         ;; grab the keys from the corresponding right-only
         ;; data frame
         le-rows (df/$conj-cols (-> ro-rows
                                      (df/$col right-on)
                                      (df/$rename-cols (zipmap right-on left-on)))
                                  (empty-df (remove #(contains? (set left-on) %)
                                                    left-cols)
                                            (count right-only)))


         ;; combine the rows for each side
         left-side (reduce df/$conj-rows [li-rows lo-rows le-rows])
         right-side (reduce df/$conj-rows [ri-rows re-rows ro-rows])

         ;; avoid column name collisions with _x and _y
         [left-repl right-repl] (avoid-collisions left-on right-on left-cols right-cols)]
     (df/$conj-cols (df/$rename-cols left-side left-repl)
                      (df/$remove-cols (df/$rename-cols right-side right-repl)
                                         common-join-cols)))))
