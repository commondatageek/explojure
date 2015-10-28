(ns explojure.dataframe.validation
  (:require [explojure.util :as util]
            [clojure.set :as s]
            [clojure.test :as t]))

(defn unique? [xs]
  (= (count xs)
     (count (util/vdistinct xs))))

(defn ensure-type
  ([x t]
   (ensure-type x t "Argument x must have type t."))

  ([x t err-msg]
   (when (not= (type x) t)
     (throw (new Exception err-msg)))
   true))

(defn ensure-vvector
  ([xs]
   (ensure-vvector xs "Argument xs must be a vector of vectors."))

  ([xs err-msg]
   (ensure-type xs clojure.lang.PersistentVector err-msg)
   (doseq [x xs]
     (ensure-type x clojure.lang.PersistentVector err-msg))
   true))

(defn ensure-equal
  ([a b]
   (ensure-equal a b "Arugments a and b must be equal"))

  ([a b err-msg]
   (when (not= a b)
     (throw (new Exception err-msg)))
   true))

(defn ensure-all-equal
  ([xs]
   (ensure-all-equal xs "All elements of xs must be equal."))

  ([xs err-msg]
   (if (= (count xs) 0)
     true
     (when (not (apply = xs))
       (throw (new Exception err-msg))))
   
   true))

(defn ensure-membership
  ([x s]
   (ensure-membership x s "Argument x must be a member of set s."))

  ([x s err-msg]
   (when (not (contains? s x))
     (throw (new Exception err-msg)))
   true))

(defn ensure-unique
  ([xs]
   (ensure-unique xs "Elements of xs must be unique."))

  ([xs err-msg]
   (when (not (unique? xs))
     (throw (new Exception err-msg)))
   true))

(defn validate-columns [colnames columns]
  ;; colnames must be a vector
  (ensure-type colnames
               clojure.lang.PersistentVector
               "colnames argument must be a vector")

  ;; colnames must all be unique
  (ensure-unique colnames
                 "colnames must be unique.")
  
  ;; columns must be a vector of vectors
  (ensure-vvector columns
                  "columns argument must be a vector of vectors")

  ;; column vectors must have the same length
  (ensure-all-equal (map count columns)
                    "All columns must have the same length.")
  
  ;; colnames must have same length as columns
  (let [cn-ct (count colnames)
        c-ct (count columns)]
    (ensure-equal cn-ct
                  c-ct
                  (str "colnames argument must have same length (" cn-ct ")"
                       " as number of columns (" c-ct ").")))
  
  
  ;; all colnames must be the same type
  (ensure-all-equal (map type colnames)
                    "All colnames must have the same type.")

  ;; colname type must be String, keyword, or integer
  (when (> (count colnames) 0)
    (ensure-membership (type (first colnames))
                       #{;; strings
                         java.lang.String
                         ;; keywords
                         clojure.lang.Keyword
                         ;; integers
                         clojure.lang.BigInt
                         java.lang.Short
                         java.lang.Integer
                         java.lang.Long}
                       "All colnames must be of type string, keyword, or integer"))
  

  true)

(defn validate-rownames [rownames row-ct]
  ;; rownames must be a vector
  (ensure-type rownames
               clojure.lang.PersistentVector
               "rownames argument must be a vector")

  ;; rownames must equal row-ct
  (let [rn-ct (count rownames)]
    (ensure-equal rn-ct
                  row-ct
                  (str "rownames argument must have same length (" rn-ct ")"
                       " as number of rows (" row-ct ").")))
  
  
  ;; rownames must all be unique
  (ensure-unique rownames
                 "rownames must be unique.")
  
  ;; all rownames must be the same type
  (ensure-all-equal (map type rownames)
                    "All rownames must have the same type.")

  ;; rowname type must be String, keyword, or integer
  (when (> (count rownames) 0)
    (ensure-membership (type (first rownames))
                       #{;; strings
                         java.lang.String
                         ;; keywords
                         clojure.lang.Keyword
                         ;; integers
                         clojure.lang.BigInt
                         java.lang.Short
                         java.lang.Integer
                         java.lang.Long}
                       "All rownames must be of type string, keyword, or integer"))
  

  true)
