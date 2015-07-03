(ns explojure.vec-ops)

(defn- ensure-vector [x]
  (if (vector? x)
    x
    (vector x)))

(defn- ensure-v-length [& vectors]
  (let [vs (map ensure-vector vectors)
        max-len (apply max (map count vs))]
    (apply vector (map #(take max-len (cycle %)) vs))))

(defn V
  "Perform vector operations on two or more vectors. This essentially provides
  the same functionality as map with one major difference.  Whereas map applies
  the function only as many times as the shortest coll argument is long, V will
  ensure that all arguments are vectors and will cycle through those values in
  order to ensure that we can get through the longest vector.
  "
  [f v1 v2 & vectors]
  (let [vs (apply ensure-v-length v1 v2 vectors)]
    (apply map f vs)))
