(ns explojure.dataframe.join
  (:require [explojure.dataframe.util :as dfu]
            [explojure.util :as util]
            
            [clojure.set :as set]))

(defn row-indices
  "For each x in xs, return [x i], where i is the 0-based index."
  [xs]
  (map vector
       xs
       (range (count xs))))

(defn gen-key-row-lookup
  "Create a hash-map for keys => [indices]."
  [ri]
  (reduce (fn [m [k i]]
            (assoc m k (conj (get m k [])
                             i)))
          {}
          ri))

(defn join-selections
  "Return the row selection indices for a full outer join."
  [left-keys right-keys]
  (let [components (util/venn-components left-keys right-keys)
        [left-only in-common left-and-common right-only] components
      
        left-lkp (gen-key-row-lookup (row-indices left-keys))
        right-lkp (gen-key-row-lookup (row-indices right-keys))
        
        ;; get left- and right-only rows
        left-only-sel (flatten
                             (map #(get left-lkp %)
                                  (util/unique left-only)))
        right-only-sel (flatten
                              (map #(get right-lkp %)
                                   (util/unique right-only)))

        ;; get in-common rows
        [left-common-sel right-common-sel]
        (reduce (fn [[lefts rights] k]
                  (let [lt (get left-lkp k)
                        lt-ct (count lt)
                        rt (get right-lkp k)
                        rt-ct (count rt)]
                    [(concat lefts
                             (flatten
                              (map #(repeat rt-ct %)
                             lt)))
                     (concat rights
                             (flatten
                              (repeat lt-ct rt)))]))
                [[] []]
                (util/unique in-common))]
    
    [left-only-sel
     left-common-sel
     right-common-sel
     right-only-sel]))

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
