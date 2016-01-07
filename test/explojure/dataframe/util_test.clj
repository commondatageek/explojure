(ns explojure.dataframe.util-test
  (:require [explojure.dataframe.util :refer :all]
            [clojure.test :refer :all]))

(deftest cmb-cols-vt-test
  (testing "bad values"
    (is (thrown? Throwable (cmb-cols-vt [[1 2] [3 4]] [[1 2 3]]))
        "top and bottom must have same number of columns")
    (is (thrown? Throwable (cmb-cols-vt [[1 2] [3 4]] [[1 2] [3]]))
        "sub-sequences in each top-level sequence must have same length")
    (is (thrown? Throwable (cmb-cols-vt 7 :x))
        "arguments must be nil or sequentials"))

  (testing "nil and empty values"
    (is (= (cmb-cols-vt nil nil)     nil)     "two nils should return nil")
    (is (= (cmb-cols-vt nil [1 2 3]) [1 2 3]) "top nil should return bottom")
    (is (= (cmb-cols-vt [4 5 6] nil) [4 5 6]) "bottom nil should return top")
    (is (= (cmb-cols-vt [] [])       [])      "empty seqs should return empty vector"))

  (testing "general functionality"
    (is (= (cmb-cols-vt [[1 2] [3 4]]
                        [[5 6 7] [8 9 10]])
           [[1 2 5 6 7] [3 4 8 9 10]])
        "works as expected")))

(deftest cmb-cols-hr-test
  (testing "bad values"
    (is (thrown? Throwable (cmb-cols-hr 7 :x))
        "arguments must be nil or sequentials")
    (is (thrown? Throwable (cmb-cols-vt [[1 2] [3 4]] [[1 2] [3]]))
        "sub-sequences in each top-level sequence must have same length")
    (is (thrown? Throwable (cmb-cols-hr [[1 2] [1 2]] [[1 2 3] [1 2 3]]))
        "all columns in both top-level sequences must have same length"))

  (testing "nil and empty values"
    (is (= (cmb-cols-hr nil nil)     nil)     "two nils should return nil")
    (is (= (cmb-cols-hr nil [1 2 3]) [1 2 3]) "left nil should return right")
    (is (= (cmb-cols-hr [1 2 3] nil) [1 2 3]) "right nil should return left")
    (is (= (cmb-cols-hr [] [])       [])      "empty seqs should return empty vector"))

  (testing "general functionality"
    (is (= (cmb-cols-hr [[1 2 3] [3 4 5]]
                        [[5 6 7] [7 8 9]])
           [[1 2 3] [3 4 5] [5 6 7] [7 8 9]])
        "works as expected")))
