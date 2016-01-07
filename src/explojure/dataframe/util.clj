(ns explojure.dataframe.util
  (:require [explojure.util :as util]))

(defn nil-cols
  "Return a vector of ncol vectors, each having nrow nils."
  [ncol nrow]
  (->> nil
       (util/vrepeat nrow)
       (util/vrepeat ncol)))

(defn sseq?
  "Return true if x is a sequence having only sequences (zero ro more) as children"
  [x]
  (and (sequential? x)
       (every? sequential? x)))

(defn truthy? [x]
  (if x true false))

(defn falsey? [x]
  (if x false true))

(defn all? [xs]
  (reduce #(and %1 %2)
          xs))

(defn equal-length?
  "REturn true if every sequence x in xs has the same count. Will cause lazy sequences to be evaluated."
  [xs]
  (if (or (nil? xs)
          (= (count xs) 0))
    true
    (do
      (assert (sseq? xs))
      (apply = (map count xs)))))

(defn cmb-cols-vt [top bottom]
  {:pre [(or (nil? top) (sequential? top))
         (or (nil? bottom) (sequential? bottom))]}
  (cond (and (nil? top) (nil? bottom)) nil
        (nil? top)                     bottom
        (nil? bottom)                  top
        
        :default
        (do (assert (sseq? top))
            (assert (sseq? bottom))
            (assert (equal-length? top))
            (assert (equal-length? bottom))
            (assert (= (count top) (count bottom)))
            (util/vmap util/vconcat
                       top
                       bottom))))

(defn cmb-cols-hr [left right]
  ;; only accept nils or sequentials
  {:pre [(or (nil? left) (sequential? left))
         (or (nil? right) (sequential? right))]}
  (cond (and (nil? left) (nil? right)) nil
        (nil? left)                    right
        (nil? right)                   left
        
        :default
        (do (assert (sseq? left))
            (assert (sseq? right))
            (assert (equal-length? (concat left right)))
            (util/vconcat left right))))

(defn collate-keyvals [keyvals]
  {:pre [(sseq? keyvals)
         (or (= (count keyvals) 0)
             (and (= (count (first keyvals)) 2)
                  (equal-length? keyvals)))]}
  (reduce (fn [m [k v]]
            (assoc m k (conj (get m k []) v)))
          {}
          keyvals))

(defn empty-sequential? [x]
  (if (and (sequential? x)
           (= (count x) 0))
    true
    false))

