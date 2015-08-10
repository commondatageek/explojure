(ns explojure.join
  (:require [clojure.set :as set])
  (:require [explojure.core :as core]))

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
    (apply core/$df (interleave colnames (repeat empty-col)))))

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
         l-idx (apply gen-index (for [c left-on] (core/$col left c)))
         r-idx (apply gen-index (for [c right-on] (core/$col right c)))

         ;; choose the row indices to draw from each DF
         [left-only
          [left-inner right-inner]
          right-only] (choose-rows l-idx
                                   r-idx
                                   (filter-keys l-idx r-idx join-type))

         ;; column names in the DFs
         left-cols (core/$colnames left)
         right-cols (core/$colnames right)

         ;; intersect join columns
         common-join-cols (set/intersection (set left-on) (set right-on))

         ;; get the appropriate rows from each DF
         le-rows (empty-df left-cols (count right-only))
         lo-rows (core/$ left left-cols left-only)
         li-rows (core/$ left left-cols left-inner)

         re-rows (empty-df right-cols (count left-only))
         ro-rows (core/$ right right-cols right-only)
         ri-rows (core/$ right right-cols right-inner)

         ;; combine the rows for each side
         left-side (reduce core/$conj-rows [li-rows lo-rows le-rows])
         right-side (reduce core/$conj-rows [ri-rows re-rows ro-rows])

         ;; avoid column name collisions with _x and _y
         [left-repl right-repl] (avoid-collisions left-on right-on left-cols right-cols)]
     (core/$conj-cols (core/$rename-cols left-side left-repl)
                      (core/$remove-cols (core/$rename-cols right-side right-repl)
                                         common-join-cols)))))


(def df1 (core/$df :x [:l :a :b :b :c :c :c :d :d :d :d]
                   :y [:0 :1 :2 :2 :3 :3 :3 :4 :4 :4 :4]
                   :a [1  2  3  4  5  6  7  8  9  10 11]))
(def df2 (core/$df :x [:a :b :b :c :c :c :d :d :d :d :r]
                   :y [:1 :2 :2 :3 :3 :3 :4 :4 :4 :4 :5]
                   :a [1  2  3  4  5  6  7  8  9  10 11]))


(def result
  (join df1
        df2
        [:x :y]
        :outer))
