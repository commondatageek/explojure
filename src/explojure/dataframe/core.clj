(ns explojure.dataframe.core
  (:require [explojure.dataframe.impl.combine.conj :as cmb-conj]
            [explojure.dataframe.impl.combine.merge :as cmb-merge]
            [explojure.dataframe.impl.combine.join :as cmb-join]
            [explojure.dataframe.impl.compare :as cmp]
            [explojure.dataframe.impl.describe :as desc]
            [explojure.dataframe.impl.modify :as mod]
            [explojure.dataframe.impl.get :as raw]
            [explojure.dataframe.impl.select.core :as sel])

  (:import [explojure.dataframe.construct.DataFrame]))

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
  (row-maps [this])

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

  ;; select
  (select-cols-by-index [this cols])
  (select-rows-by-index [this rows])
  ($ [this col-spec]
     [this col-spec row-spec])

  ;; drop
  (drop-cols-by-index [this cols])
  (drop-rows-by-index [this rows])
  ($- [this col-spec]
      [this col-spec row-spec])
  (drop-duplicates [this]
                   [this col-spec]
                   [this col-spec keep-fn])

  ;; combine dataframes
  (conj-cols [this right])
  (conj-rows [this bottom])
  (merge-frames [this other resolution-fn])
  (join-frames [this right on join-type]
               [this left-on right right-on join-type]))

(extend-type explojure.dataframe.construct.DataFrame
  ;; -------
  Tabular
  ;; -------

  ;; general attributes and raw access
  (colnames [this]
    (raw/colnames this))
  
  (rownames [this]
    (raw/rownames this))
  
  (ncol [this]
    (raw/ncol this))
  
  (nrow [this]
    (raw/nrow this))
  
  (lookup-names [this axis xs]
    (raw/lookup-names this axis xs))
  
  (col-vectors [this]
    (raw/col-vectors this))
  
  (row-vectors [this]
    (raw/row-vectors this))

  (row-maps [this]
    (raw/row-maps this))

  ;; descriptive
  (not-nil [this]
    (desc/not-nil this))

  ;; comparative
  (equal? [this other]
    (cmp/equal? this other))

  ;; selection
  (select-cols-by-index [this cols]
    (sel/select-cols-by-index this cols))
  
  (select-rows-by-index [this rows]
    (sel/select-rows-by-index this rows))
  
  ($
    ([this col-spec]
     (sel/$ this col-spec))
    ([this col-spec row-spec]
     (sel/$ this col-spec row-spec)))
  
  (drop-cols-by-index [this cols]
    (sel/drop-cols-by-index this cols))
  
  (drop-rows-by-index [this rows]
    (sel/drop-rows-by-index this rows))
  
  ($-
    ([this col-spec]
     (sel/$- this col-spec))
    ([this col-spec row-spec]
     (sel/$- this col-spec row-spec)))

  (drop-duplicates
    ([this]
     (sel/drop-duplicates this))
    ([this col-spec]
     (sel/drop-duplicates this col-spec))
    ([this col-spec keep-fn]
     (sel/drop-duplicates this col-spec keep-fn)))

  ;; modification
  (add-col
    ([this colname column]
     (mod/add-col this colname column))
    ([this add-map]
     (mod/add-col this add-map)))
  
  (rename-col
    ([this old-colname new-colnames]
     (mod/rename-col this old-colname new-colnames))
    ([this rename-map]
     (mod/rename-col this rename-map)))
  
  (replace-col
    ([this colname column]
     (mod/replace-col this colname column))
    ([this replace-map]
     (mod/replace-col this replace-map)))
  
  (set-col
    ([this colname column]
     (mod/set-col this colname column))
    ([this set-map]
     (mod/set-col this set-map)))
  
  (set-colnames [this new-colnames]
    (mod/set-colnames this new-colnames))

  (set-rownames [this new-rownames]
    (mod/set-rownames this new-rownames))

  ;; combination
  (conj-cols [this right]
    (cmb-conj/conj-cols this right))

  (conj-rows [this bottom]
    (cmb-conj/conj-rows this bottom))

  (merge-frames [this other resolution-fn]
    (cmb-merge/merge-frames [this other resolution-fn]))

  (join-frames
    ([this right on join-type]
     (cmb-join/join-frames this right on join-type))
    ([this left-on right right-on join-type]
     (cmb-join/join-frames this left-on right right-on join-type))))
