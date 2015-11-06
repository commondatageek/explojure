(ns explojure.dataframe.util
  (:require [explojure.util :as util]))

(defprotocol Tabular
  "The Tabular protocol represents tabular data.  (Rows and columns)."

  ;; general attributes
  (colnames [this])
  (rownames [this])
  (ncol [this])
  (nrow [this])
  (not-nil [this])

  (equal? [this other])

  (lookup-names [this axis xs])

  (col-vectors [this])
  (row-vectors [this])

  ;; select
  (select-cols-by-index [this cols])
  (select-rows-by-index [this rows])
  ($ [this col-spec]
     [this col-spec row-spec])

  ;; drop
  (drop-cols-by-index [this cols])
  (drop-rows-by-index [this cols])
  ($- [this col-spec]
      [this col-spec row-spec])

  ;; modify
  (add-col [this colname column]
           [this add-map])

  (rename-col [this old-colname new-colname]
              [this rename-map])

  (replace-col [this colname column]
               [this replace-map])

  (set-col [this colname column]
           [this set-map])

  ;; index modifications
  (set-colnames [this new-colnames])
  (set-rownames [this new-rownames])

  ;; combine dataframes
  (conj-cols [this right])
  (conj-rows [this bottom])
  (merge-frames [this other resolution-fn])
  (join-frames [this right on join-type]
               [this right left-on right-on join-type]))

(defn nil-cols
  "Return a vector of ncol vectors, each having nrow nils."
  [ncol nrow]
  (->> nil
       (util/vrepeat nrow)
       (util/vrepeat ncol)))

