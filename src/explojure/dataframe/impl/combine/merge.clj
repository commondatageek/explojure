(ns explojure.dataframe.impl.combine.merge
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.get :as raw]
            [explojure.dataframe.impl.select.core :as sel]
            [explojure.dataframe.util :as dfu]
            [explojure.util :as util]))

(def conflict-resolution-strategies
  {;; always return left value
   :left (fn [_ _ left _] left)

   ;; always return right value
   :right (fn [_ _ _ right] right)

   ;; return left value if not nil; else right; else nil
   :left-not-nil (fn [_ _ left right]
                   (if left left right))

   ;; return right value if not nil; else left; else nil
   :right-not-nil (fn [_ _ left right]
                    (if right right left))})


(defn merge-frames [df1 df2 resolution-fn]
  (cond (or (nil? df2)
            (= (raw/ncol df2) 0))
        df1

        (or (nil? df1)
            (= (raw/ncol df1) 0))
        df2

        :default
        (let [df1-colnames (raw/colnames df1)
              df2-colnames (raw/colnames df2)
              df1-rownames (raw/rownames df1)
              df2-rownames (raw/rownames df2)]

          ;; some input validation
          (assert (and df1-colnames df2-colnames
                       df1-rownames df2-rownames)
                  (str "merge: both DataFrames must have both rownames and colnames:\n"
                       "df1 colnames: " (if df1-colnames "supplied" "missing") "\n"
                       "df1 rownames: " (if df1-rownames "supplied" "missing") "\n"
                       "df2 colnames: " (if df2-colnames "supplied" "missing") "\n"
                       "df2 rownames: " (if df2-rownames "supplied" "missing")))

          (let [;; set components
                [rn-df1-only rn-in-common rn-df1-and-common rn-df2-only]
                (util/venn-components df1-rownames df2-rownames)
                [cn-df1-only cn-in-common cn-df1-and-common cn-df2-only]
                (util/venn-components df1-colnames df2-colnames)

                ;; overlap / conflict
                conflict-columns (for [c cn-in-common]
                                   [c rn-in-common])

                ;; resolve conflicts
                resolved (vec (for [[col rownames] conflict-columns]
                                (util/vmap #(apply resolution-fn %)
                                           (map vector
                                                (repeat col)
                                                rownames
                                                (sel/$ df1 col rownames)
                                                (sel/$ df2 col rownames)))))

                combined-columns
                (dfu/cmb-cols-vt
                 (dfu/cmb-cols-hr (raw/col-vectors (sel/$ df1 cn-df1-only rn-df1-only))
                                  (raw/col-vectors (sel/$ df1 cn-in-common rn-df1-only))
                                  (dfu/nil-cols (count cn-df2-only) (count rn-df1-only)))

                 (dfu/cmb-cols-hr (raw/col-vectors (sel/$ df1 cn-df1-only rn-in-common))
                                  resolved
                                  (raw/col-vectors (sel/$ df2 cn-df2-only rn-in-common)))
                 
                 (dfu/cmb-cols-hr (dfu/nil-cols (count cn-df1-only) (count rn-df2-only))
                                  (raw/col-vectors (sel/$ df2 cn-in-common rn-df2-only))
                                  (raw/col-vectors (sel/$ df2 cn-df2-only rn-df2-only))))
                
                combined-colnames (util/vconcat cn-df1-only cn-in-common cn-df2-only)
                
                combined-rownames (util/vconcat rn-df1-only rn-in-common rn-df2-only)]
            
            (sel/$ (ctor/new-dataframe combined-colnames
                                       combined-columns
                                       combined-rownames)
                   (concat cn-df1-and-common cn-df2-only)
                   (concat rn-df1-and-common rn-df2-only))))))
