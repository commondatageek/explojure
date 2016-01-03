(ns explojure.dataframe.impl.describe
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.get :as raw]))

(defn not-nil
  [^explojure.dataframe.construct.DataFrame this]
  (ctor/new-dataframe [:column :count]
                      [raw/colnames
                       (vec
                        (for [c (raw/columns)]
                          (count
                           (keep #(not (nil? %)) c))))]))
