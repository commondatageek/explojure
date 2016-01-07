(ns explojure.dataframe.impl.combine.conj
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.get :as raw]
            [explojure.dataframe.impl.select.core :as sel]
            [explojure.dataframe.util :as dfu]
            [explojure.util :as util]

            [clojure.set :as s]))

(defn conj-cols [left right]
  (let [lt-nil (or (nil? left) (= (raw/ncol left) 0))
        rt-nil (or (nil? right) (= (raw/ncol right) 0))]

    (cond (and lt-nil rt-nil) nil
          lt-nil              right
          rt-nil              left

          :default
          (let [left-colnames (raw/colnames left)
                right-colnames (raw/colnames right)
                left-rownames (raw/rownames left)
                right-rownames (raw/rownames right)
                
                colname-conflicts (s/intersection (set left-colnames)
                                                  (set right-colnames))
                equal-nrow (= (raw/nrow left)
                              (raw/nrow right))

                left-has-rownames (not (nil? left-rownames))
                right-has-rownames (not (nil? right-rownames))
                
                align-rownames (and left-has-rownames
                                    right-has-rownames)]
            
            (assert (= (count colname-conflicts) 0)
                    (str "conj-cols: colnames must be free of conflicts (" colname-conflicts "). Consider using (merge-frames) if appropriate."))
            
            (if align-rownames
              ;; if both DFs have rownames
              (let [[left-only in-common left-and-common right-only]
                    (util/venn-components left-rownames
                                          right-rownames)
                    
                    combined-colnames
                    (util/vconcat left-colnames right-colnames)

                    combined-columns
                  (reduce dfu/cmb-cols-vt
                          [(dfu/cmb-cols-hr (raw/col-vectors (sel/$ left nil left-only))
                                            (dfu/nil-cols (raw/ncol right) (count left-only)))
                           (dfu/cmb-cols-hr (raw/col-vectors (sel/$ left nil in-common))
                                            (raw/col-vectors (sel/$ right nil in-common)))
                           (dfu/cmb-cols-hr (dfu/nil-cols (raw/ncol left) (count right-only))
                                            (raw/col-vectors (sel/$ right nil right-only)))])
                    
                    combined-rownames
                    (util/vconcat left-only in-common right-only)]
                
                (sel/$ (ctor/new-dataframe combined-colnames
                                           combined-columns
                                           combined-rownames)
                       nil
                       (concat left-and-common right-only)))

              ;; if one or none of the DFs have rownames
              (do
                (assert equal-nrow
                        "conj-cols: if both dataframes do not each have rownames, the two dataframes must have the same number of rows.")
                (let [combined-colnames (util/vconcat left-colnames
                                                      right-colnames)
                      
                      combined-columns (util/vconcat (raw/col-vectors left)
                                                     (raw/col-vectors right))
                      
                      ;; at this point we know it is not true that both DFs have rownames
                      ;; so use the one that has them.  Or nil if neither has them.
                      use-rownames (if (raw/rownames left)
                                     (raw/rownames left)
                                     (raw/rownames right))]

                  (ctor/new-dataframe combined-colnames
                                      combined-columns
                                      use-rownames))))))))

(defn conj-rows [top bottom]
  (cond (or (nil? bottom)
            (= (raw/ncol bottom) 0))
        top

        (or (nil? top)
            (= (raw/ncol top) 0))
        bottom

        :default
        (let [top-rownames (raw/rownames top)
              bottom-rownames (raw/rownames bottom)
              
              top-has-rownames (not (nil? top-rownames))
              bottom-has-rownames (not (nil? bottom-rownames))

              both-have-rownames (and top-has-rownames
                                      bottom-has-rownames)
              both-no-rownames (and (not top-has-rownames)
                                    (not bottom-has-rownames))
              
              rowname-conflicts (s/intersection (set top-rownames)
                                                (set bottom-rownames))]
          
          (assert (or both-have-rownames both-no-rownames)
                  (str "conj-rows: DataFrames must both have rownames, or they must both not have rownames in order to maintain the integrity of rowname semantics."))
          (when both-have-rownames
            (assert (= (count rowname-conflicts) 0)
                    (str "conj-rows: rownames must be free of conflicts (" rowname-conflicts "). Consider using (merge-frames) if appropriate.")))

          (let [top-colnames (raw/colnames top)
                bottom-colnames (raw/colnames bottom)

                [top-only in-common top-and-common bottom-only]
                (util/venn-components top-colnames
                                      bottom-colnames)
                cmb-colnames (util/vconcat top-only in-common bottom-only)
                
                cmb-columns
                (dfu/cmb-cols-vt (reduce dfu/cmb-cols-hr
                                         [(raw/col-vectors (sel/$ top top-only nil))
                                          (raw/col-vectors (sel/$ top in-common nil))
                                          (dfu/nil-cols (count bottom-only)
                                                        (raw/nrow top))])
                                 (reduce dfu/cmb-cols-hr
                                         [(dfu/nil-cols (count top-only)
                                                        (raw/nrow bottom))
                                          (raw/col-vectors (sel/$ bottom in-common nil))
                                          (raw/col-vectors (sel/$ bottom bottom-only nil))]))
                
                cmb-rownames (if both-have-rownames
                               (util/vconcat top-rownames
                                             bottom-rownames)
                               nil)]
            
            (sel/$ (ctor/new-dataframe cmb-colnames
                                       cmb-columns
                                       cmb-rownames)
                   (concat top-and-common bottom-only)
                   nil)))))
