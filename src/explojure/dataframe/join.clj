(ns explojure.dataframe.join2
  (:require [explojure.dataframe.core :as df]
            [explojure.dataframe.util :as dfu]
            [explojure.util :as u]
            [clojure.set :as s]))

(defn generate-index [df on]
  (reduce (fn [m [i k]]
            (assoc m k (conj (get m k []) i)))
          {}
          (map-indexed vector
                       (dfu/row-vectors
                        (dfu/$ df on nil)))))

(defn nil-df [cols nrows]
  (let [ncols     (count cols)
        empty-col (u/vrepeat nrows nil)]
    (df/new-dataframe cols (u/vrepeat ncols empty-col))))

(defmulti  avoid-collision (fn [c sfx] (type c)))
(defmethod avoid-collision clojure.lang.Keyword   [c sfx] (keyword (str (name c) "-" sfx)))
(defmethod avoid-collision java.lang.String       [c sfx] (str c "_" sfx))

(defn- rename-strategy [left-colnames right-colnames on-l on-r]
  ;; don't care about columns that don't have conflicts.

  ;; for those that do, we need to consider these 4 cases
  ;;  - neither is a join column                            => rename c_x, c_y
  ;;  - one is a join column, one is not                    => rename c_x, c_y
  ;;  - both are join columns, but not the same join column => rename c_x, c_y
  ;;  - both are join columns, both are the same            => use only left values for
  ;;                                                           left-only, in-common, and
  ;;                                                           right values for right-only

  ;; FIXME - This isn't going to work for number column names.
  ;; FIXME - joins might have problems if one DF has keyword colnames and the other has strings
  (let [same-keys (set (u/filter-ab #(apply = %)
                                    (map vector on-l on-r)
                                    on-l))
        collisions (remove same-keys
                           (s/intersection (set left-colnames)
                                           (set right-colnames)))
        gen-rnm-map (fn [collisions sfx]
                      (reduce (fn [m [old new]]
                                (assoc m old new))
                              {}
                              (map vector
                                   collisions
                                   (map #(avoid-collision % sfx) collisions))))]
    [(vec same-keys)
     (gen-rnm-map collisions "x")
     (gen-rnm-map collisions "y")]))

(defn- get-outer-indices [idx keys]
  (->> (map idx keys) (flatten) (sort)))

(defn- get-inner-indices [lt-idx rt-idx inner-k]
  (let [inner-i (sort-by first
                         (reduce concat
                                 (map #(u/cart-prod (lt-idx %)
                                                    (rt-idx %))
                                      inner-k)))]
    [(u/vmap first inner-i)
     (u/vmap second inner-i)]))

(defn join
  ([left right on join-type]
   (join left on right on join-type))

  ([left on-l right on-r join-type]
   {:pre [(every? #(contains? (set (dfu/colnames left)) %) on-l)
          (every? #(contains? (set (dfu/colnames right)) %) on-r)
          (contains? #{:inner :left :right :outer} join-type)]}
   (let [;; get index of which keys are on which rows
         lt-idx (generate-index left on-l)
         rt-idx (generate-index right on-r)
         
         ;; key sets
         [lt-outer-k inner-k _ rt-outer-k]
         (u/venn-components (keys lt-idx) (keys rt-idx))
         
         ;; get selection row indices
          lt-outer-i  (get-outer-indices lt-idx lt-outer-k)
         [lt-inner-i
          rt-inner-i] (get-inner-indices lt-idx rt-idx inner-k)
          rt-outer-i  (get-outer-indices rt-idx rt-outer-k)

         ;; given join-type, which outer sets to use in the final result?
         join-sets (get {:inner #{          :inner          }
                          :left  #{:lt-outer :inner          }
                          :right #{          :inner :rt-outer}
                          :outer #{:lt-outer :inner :rt-outer}}
                         join-type)

         ;; find out which key columns are common between left and right
         ;; get rename maps for left and right
         [same-keys rnm-l rnm-r],
         (rename-strategy (dfu/colnames left)
                          (dfu/colnames right)
                          on-l
                          on-r)]


     (df/conj-cols (as-> (reduce df/conj-rows
                                 [(when (join-sets :lt-outer)
                                    (dfu/$ left nil lt-outer-i))
                                  (when (join-sets :inner)
                                    (dfu/$ left nil lt-inner-i))
                                  (when (join-sets :rt-outer)
                                    (as-> (nil-df (dfu/colnames left)
                                                  (count rt-outer-i))
                                        ln
                                      (dfu/set-col ln
                                                   (apply hash-map
                                                          (interleave same-keys
                                                                      (map #(dfu/$ right % rt-outer-i)
                                                                           same-keys))))))])
                       %
                     (dfu/rename-col % rnm-l))
                   
                   (as-> (reduce df/conj-rows
                                 [(when (join-sets :lt-outer)
                                    (nil-df (dfu/colnames right)
                                            (count lt-outer-i)))
                                  (when (join-sets :inner)
                                    (dfu/$ right nil rt-inner-i))
                                  (when (join-sets :rt-outer)
                                    (dfu/$ right nil rt-outer-i))])
                       %
                     (dfu/rename-col % rnm-r)
                     (dfu/$- % same-keys nil))))))

