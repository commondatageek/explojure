(ns explojure.dataframe.print
  (import explojure.dataframe.core.DataFrame
          java.util.regex.Matcher)
  (:require [explojure.dataframe.core :as df-core]
            [explojure.dataframe.util :as dfu]
            [explojure.util :as util]))

(defn sp [x] (str " " x " "))
(defn qu [x] (str "\"" x "\""))


;; find max width of column
(defmulti width string?)
(defmethod width true [x] (+ (count x) 2))
(defmethod width false [x] (count (str x)))
(defn max-width [xs] (apply max (map width xs)))

;; constrain column width
(defn re-qr [replacement]
  (Matcher/quoteReplacement replacement))
(defmulti ensure-width (fn [width x] (string? x)))
(defmethod ensure-width true [width x]
  (let [x (clojure.string/replace x #"\n" (re-qr "\n"))
        x (clojure.string/replace x #"\r" (re-qr "\r"))
        str-len (count x)
        qu-len (+ str-len 2)
        pad-len (max 0 (- width qu-len))]
    (if (= pad-len 0)
      (if (= qu-len width)
        (qu x)
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

;; put | dividers between column values
(defn col-dividers [xs] (str "| "  (apply str (interpose " | " xs)) " |\n"))

;; here's the magic to converta dataframe to textual format
(defn df->str [df]
  (let [colnames (dfu/colnames df)
        columns (dfu/col-vectors df)
        new-df (df-core/new-dataframe colnames
                                      (util/vmap (fn [col]
                                                   (let [mx-wd (max-width col)
                                                         wd (min mx-wd 30)]
                                                     (util/vmap #(ensure-width wd %)
                                                                col)))
                                                 (util/vmap #(util/vflatten (vector %1 %2))
                                                            colnames
                                                            columns)))
        rows (dfu/row-vectors new-df)]
    (let [headers (first rows)
          data-rows (rest rows)
          header-line (col-dividers headers)]
      ;; print the header line and dividers
      (apply str (concat [(str (apply str (repeat (- (count header-line) 1) "-")) "\n")
                          header-line
                          (str (apply str (repeat (- (count header-line) 1) "-")) "\n")]
                         (for [r data-rows]
                           (col-dividers r))
                         [(str (apply str (repeat (- (count header-line) 1) "-")) "\n")])))))


;; to make DataFrames display correctly at the REPL
(defmethod print-method explojure.dataframe.core.DataFrame [x ^java.io.Writer w]
  (.write w (df->str x)))

;; print a dataframe
(defn print-df [df] (print (df->str df)))


