(ns explojure.dataframe.specifiers
  (:require [explojure.dataframe.util :as dfu]
            [explojure.util :as util]))

(declare interpret-spec)

(defn interpret-nil-spec [df axis nl] nil)

(defn interpret-empty-spec [df axis mpt] [])

(defn interpret-fn-spec [df axis f]
  (when (not= (count f) 1)
    (throw (new Exception "Only one function can be passed per specifier.")))
  (let [f (first f)]
    (interpret-spec df
                    axis
                    (map boolean
                         (map f (case axis
                                  :cols (dfu/col-vectors df)
                                  :rows (dfu/row-vectors df)))))))

(defn interpret-int-spec [df axis ints]
  (let [n (dfu/lookup-names df axis ints)]
    (if (util/no-nil? n)
      n
      ints)))

(defn interpret-bool-spec [df axis bools]
  (when (not= (count bools) (case axis
                              :cols (dfu/ncol df)
                              :rows (dfu/nrow df)))
    (throw (new Exception "Boolean specifiers must have length equal to ncol or nrow.")))
  (util/where bools))

(defn interpret-name-spec [df axis names]
  (let [n (dfu/lookup-names df axis names)]
    (if (util/no-nil? n)
      n
      (throw
       (new Exception
            (str "The given DataFrame does not have the following "
                 (case axis
                   :cols "column"
                   :rows "row")
                 " names: "
                 (vec (util/filter-ab (map not (map boolean n))
                                      names))))))))

;; TODO: This conditional code looks brittle to me.
;;   Think about if there is a better way to do this.
(defn interpret-spec [df axis spec]
  (cond
    ;; handle nils
    (nil? spec)
    (interpret-nil-spec df axis spec)
    
    ;; handle empty sequentials
    (and (sequential? spec)
         (= (count spec) 0))
    (interpret-empty-spec df axis spec)
    
    ;; handle everything else
    :else
    (let [spec (util/make-vector spec)
          f (first spec)]
      (when (not (util/same-type spec))
        (throw
         (new Exception "All elements of spec must be of the same type.")))
      (cond
        (fn? f)                       (interpret-fn-spec df axis spec)
        (integer? f)                  (interpret-int-spec df axis spec)
        (util/boolean? f)             (interpret-bool-spec df axis spec)
        (or (string? f) (keyword? f)) (interpret-name-spec df axis spec)
        :else (throw (new Exception (str "Specifiers must be nil, [], fn, integer, boolean, string, or keyword. Received " (type f))))))))


(defn multi-specifier?
  "If x is a specifier that denotes multiplicity, return true. Else false."
  [x]
  (or (sequential? x)
      (nil? x)
      (fn? x)))

