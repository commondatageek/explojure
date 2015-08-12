(ns explojure.print
  (require [explojure.core :as core]))

(defn sp [x] (str " " x " "))
(defn qu [x] (str "\"" x "\""))


;; find max width of column
(defmulti width string?)
(defmethod width true [x] (+ (count x) 2))
(defmethod width false [x] (count (str x)))
(defn max-width [xs] (apply max (map width xs)))

;; constrain column width
(defmulti ensure-width (fn [width x] (string? x)))
(defmethod ensure-width true [width x]
  (let [str-len (count x)
        qu-len (+ str-len 2)
        pad-len (max 0 (- width qu-len))]
    (if (= pad-len 0)
      (if (= qu-len width)
        x
        (qu (str (subs x 0 (- width 3)) ">")))
      (str (qu x) (apply str (repeat pad-len " "))))))
(defmethod ensure-width false [width x]
  (let [str-repr (str x)
        str-len (count str-repr)
        pad-len (max 0 (- width str-len))]
    (if (= pad-len 0)
      (if (= str-len width)
        str-repr
        (str (subs str-repr 0 (- width 1)) ">"))
      (str (apply str (repeat pad-len " ")) str-repr))))

(defn col-dividers [xs] (str "| "  (apply str (interpose " | " xs)) " |"))

(defn print-df [df]
  (let [[columns data-hash] (core/$raw df)
        new-df (core/->DataFrame columns
                                 (reduce (fn [dh c]
                                           (let [old-col (vec (concat [c] (get dh c)))
                                                 new-col (map #(ensure-width
                                                                (min (max-width old-col)
                                                                     30)
                                                                %)
                                                              old-col)]
                                             (assoc dh c new-col)))
                                         data-hash
                                         columns))
        rows (core/$rows new-df)]
    (let [headers (first rows)
          data-rows (rest rows)
          header-line (col-dividers headers)]
      ;; print the header line and dividers
      (println (apply str (repeat (count header-line) "-")))
      (println header-line)
      (println (apply str (repeat (count header-line) "-")))
      (doseq [r data-rows]
        (println (col-dividers r)))
      (println (apply str (repeat (count header-line) "-"))))))
