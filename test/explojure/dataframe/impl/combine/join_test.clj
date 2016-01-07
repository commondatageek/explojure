(ns explojure.dataframe.impl.combine.join-test
  (:require [explojure.dataframe.construct :as ctor]
            [explojure.dataframe.impl.combine.join :refer :all]
            [explojure.dataframe.impl.compare :as cmp]
            [clojure.test :refer :all]))

(deftest join-frames-test3
  (testing "all join sets present in result"
    (let [;; test dataframes
          df-a (ctor/df :a [1 1 2 2 3 3]
                        :b [1 2 3 4 5 6]
                        :c ["Adam" "Benjamin" "Caleb" "Daniel" "Ephraim" "Frank"])
          df-b (ctor/df :a [1 2 3]
                        :b [4 5 6]
                        :c ["Gideon" "Hepsibah" "Ichabod"])
          
          ;; expected join results
          inner-exp (ctor/df :a [3]
                             :b [6]
                             :c-x ["Frank"]
                             :c-y ["Ichabod"])

          outer-exp (ctor/df :a [1 1 2 2 3 3 1 2]
                             :b [1 2 3 4 5 6 4 5]
                             :c-x ["Adam" "Benjamin" "Caleb" "Daniel" "Ephraim" "Frank" nil nil]
                             :c-y [nil nil nil nil nil "Ichabod" "Gideon" "Hepsibah"])

          left-exp  (ctor/df :a [1 1 2 2 3 3]
                             :b [1 2 3 4 5 6]
                             :c-x ["Adam" "Benjamin" "Caleb" "Daniel" "Ephraim" "Frank"]
                             :c-y [nil nil nil nil nil "Ichabod"])

          right-exp (ctor/df :a [3 1 2]
                             :b [6 4 5]
                             :c-x ["Frank" nil nil]
                             :c-y ["Ichabod" "Gideon" "Hepsibah"])]
      
      (is (cmp/equal? (join-frames df-a [:a :b]
                                   df-b [:a :b]
                                   :inner)
                      inner-exp)
          "inner join, all join result sets")
      (is (cmp/equal? (join-frames df-a [:a :b]
                                   df-b [:a :b]
                                   :outer)
                      outer-exp)
          "outer join, all join result sets")
      (is (cmp/equal? (join-frames df-a [:a :b]
                                   df-b [:a :b]
                                   :left)
                      left-exp)
          "left join, all join result sets")
      (is (cmp/equal? (join-frames df-a [:a :b]
                                   df-b [:a :b]
                                   :right)
                      right-exp)
          "right join, all join result sets")))

  (testing "no inner"
    (let [df-a (ctor/df :a [1 2 3]
                        :b [\a \b \c])
          df-b (ctor/df :a [4 5 6]
                        :b [\d \e \f])

          ;; expected join results
          inner-exp (ctor/df :a []
                             :b-x []
                             :b-y [])
          outer-exp (ctor/df :a [1 2 3 4 5 6]
                             :b-x [\a \b \c nil nil nil]
                             :b-y [nil nil nil \d \e \f])
          left-exp (ctor/df :a [1 2 3]
                            :b-x [\a \b \c]
                            :b-y [nil nil nil])
          right-exp (ctor/df :a [4 5 6]
                             :b-x [nil nil nil]
                             :b-y [\d \e \f])]
      
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :inner)
                      inner-exp)
          "inner join, no inner result set")
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :outer)
                      outer-exp)
          "outer join, no inner result set")
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :left)
                      left-exp)
          "left join, no inner result set")
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :right)
                      right-exp)
          "right join, no inner result set")))

  (testing "no outer"
    (let [df-a (ctor/df :a [1 2 3]
                        :b [\a \b \c])
          df-b (ctor/df :a [1 2 3]
                        :b [\d \e \f])

          ;; expected join results
          only-exp (ctor/df :a [1 2 3]
                             :b-x [\a \b \c]
                             :b-y [\d \e \f])]
      
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :inner)
                      only-exp)
          "inner join, no inner result set")
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :outer)
                      only-exp)
          "outer join, no inner result set")
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :left)
                      only-exp)
          "left join, no inner result set")
      (is (cmp/equal? (join-frames df-a [:a]
                                   df-b [:a]
                                   :right)
                      only-exp)
          "right join, no inner result set"))))

