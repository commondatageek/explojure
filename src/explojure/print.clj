(ns explojure.print)

(defn sp [x] (str " " x " "))
(defn qu [x] (str "\"" x "\""))

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


