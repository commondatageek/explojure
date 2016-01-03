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
  (every? identity
          (concat [(sequential? x)]
                  (for [el x]
                    (sequential? el)))))
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
  {:pre [(every? sequential? xs)]}
  (apply = (map count xs)))

(defn cmb-cols-vt
  [& args]
  {:pre [(every? sseq? args)
         (equal-length? args)
         (every? equal-length? args)]}
  (util/vmap util/vflatten
             (apply util/vmap vector args)))

(defn cmb-cols-hr [& args]
  {:pre [(every? sseq? args)
         (every? equal-length? args)]}
  (apply util/vconcat args))

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

