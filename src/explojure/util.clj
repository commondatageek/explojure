(ns explojure.util)

(defn equal-length
  "For one or more vectors, determine if all vectors have the same length."
  [v1 & vs]
  (apply = (map count (concat [v1] vs))))

(defn t
  "Transform vectors of vectors."
  [vs]
  {:pre [apply equal-length vs]}
  ;; for all minor vectors in the top-level vector
  (loop [vs vs
         result (array-map)]
    (if (seq vs)
      (recur (next vs)
             ;; for all elements in each minor vector
             (loop [v-elements (first vs)
                    result result
                    i 0]
               (if (seq v-elements)
                 (recur (next v-elements)
                        (assoc result
                               i
                               (conj (get result i [])
                                     (first v-elements)))
                        (inc i))
                 result)))
      ;; re
      (mapv #(get result %)
            (sort (keys result))))))
