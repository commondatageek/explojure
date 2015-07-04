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

(defn rand-n
  ([n] (rand-n 1 n))
  ([max n]
   (vec (repeatedly n #(rand max)))))

(defn random-seq
  "
  Return a lazy sequence where each element is a size 1 random sample
  from coll. The returned sequence will have the same theoretical frequency
  distribution as coll.

  Performance Notes
   - processes 10,000,000 in about 3.5 seconds, where (count coll) is 26
   - processes 10,000,000 in about 6.5 seconds, where (count coll) is 10e6.
  "
  [coll]
  (lazy-seq
   (cons (rand-nth coll) (random-seq coll))))

(defn choose
  "Need to allow with/without replacement options."
  [x n]
  (let [population (vec x)
        indices (take n (shuffle (range (count population))))]
    (mapv population indices)))

(defn maybe?
  "
  Written as a predicate function for filter that randomly returns true
  with a certain probability (default 0.5).

  Arguments
   - _: placeholder for when maybe? is used as a predicate for a filter
   - p: change the probability maybe? will return true
  "
  ([] (maybe? nil 0.5))
  ([_] (maybe? nil 0.5))
  ([_ p] (< (rand) p)))


(defn filter-ab
  "
  Return the elements of sequence b for which the application of pred to the
  corresponding elements in sequence b yields truthy results.

  If pred is not given, it defaults to \"identity,\" which means that for
  every element in a that is truthy, the corresponding element of b will be
  returned.
  "
  ([a b] (filter-ab identity a b))
  ([pred a b]
   (map #(second %)
        (filter #(pred (first %))
                (map vector a b)))))

(defn where
  "
  Return the 0-based indices where coll is truthy.

  Notes
   - processes 10,000,000 in approximately 23 seconds
  "
  [coll]
  (filter-ab coll (range (count coll))))
