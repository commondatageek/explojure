(ns explojure.dataframe.impl.compare
  (:require [explojure.dataframe.construct]))

(defn equal?
  "Uses colnames, columns, and rownames as the basis for equality
  comparison with another DataFrame."
  [^explojure.dataframe.construct.DataFrame this
   ^explojure.dataframe.construct.DataFrame other]
  (and (= (.colnames this) (.colnames other))
       (= (.rownames this) (.rownames other))
       (= (.columns this) (.columns other))))
