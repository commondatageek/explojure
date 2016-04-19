(ns explojure.dataframe.impl.combine.join
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.combine.conj :as cmb-conj]
            [explojure.dataframe.impl.modify :as mod]
            [explojure.dataframe.impl.get :as raw]
            [explojure.dataframe.impl.select.core :as sel]
            [explojure.dataframe.util :as dfu]
            [explojure.util :as u]
            [clojure.set :as s]))

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
  (->> (u/vmap idx keys) (u/vflatten)))

(defn get-inner-indices [lt-idx rt-idx inner-k]
  (map persistent!
       (reduce (fn [[result-x result-y] k]
                 (let [xs (lt-idx k)
                       ys (rt-idx k)]
                   (if (and (= (count xs) 1)
                            (= (count ys) 1))
                     [(conj! result-x (first xs))
                      (conj! result-y (first ys))]
                     (reduce (fn [[result-x result-y] x]
                               (reduce (fn [[result-x result-y] y]
                                         [(conj! result-x x)
                                          (conj! result-y y)])
                                       [result-x result-y]
                                       ys))
                             [result-x result-y]
                             xs))))
               [(transient [])
                (transient [])]
               inner-k)))

(defn join-frames
  ([left right on join-type]
   (join-frames left on right on join-type))

  ([left on-l right on-r join-type]
   {:pre [(every? #(contains? (set (raw/colnames left)) %) on-l)
          (every? #(contains? (set (raw/colnames right)) %) on-r)
          (contains? #{:inner :left :right :outer} join-type)]}
   (let [;; get index of which keys are on which rows
         lt-idx (u/index-seq (raw/row-vectors (sel/$ left on-l nil)))
         rt-idx (u/index-seq (raw/row-vectors (sel/$ right on-r nil)))
         
         ;; key sets
         [lt-outer-k inner-k _ rt-outer-k]
         (u/venn-components (keys lt-idx) (keys rt-idx))
         
         ;; get selection row indices
         lt-outer-i  (get-outer-indices lt-idx lt-outer-k)
         [lt-inner-i rt-inner-i] (get-inner-indices lt-idx rt-idx inner-k)
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
         (rename-strategy (raw/colnames left)
                          (raw/colnames right)
                          on-l
                          on-r)]

     (cmb-conj/conj-cols (as-> (reduce cmb-conj/conj-rows
                                       [(when (join-sets :lt-outer)
                                          (sel/$ left nil lt-outer-i))
                                        (when (join-sets :inner)
                                          (sel/$ left nil lt-inner-i))
                                        (when (join-sets :rt-outer)
                                          (as-> (ctor/nil-df (raw/colnames left)
                                                             (count rt-outer-i))
                                              ln
                                            (mod/set-col ln
                                                         (apply hash-map
                                                                (u/vinterleave same-keys
                                                                               (u/vmap #(sel/$ right % rt-outer-i)
                                                                                       same-keys))))))])
                             %
                           (mod/rename-col % rnm-l))
                         
                         (as-> (reduce cmb-conj/conj-rows
                                       [(when (join-sets :lt-outer)
                                          (ctor/nil-df (raw/colnames right)
                                                       (count lt-outer-i)))
                                        (when (join-sets :inner)
                                          (sel/$ right nil rt-inner-i))
                                        (when (join-sets :rt-outer)
                                          (sel/$ right nil rt-outer-i))])
                             %
                           (mod/rename-col % rnm-r)
                           (sel/$- % same-keys nil))))))
