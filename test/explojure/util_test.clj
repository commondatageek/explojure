(ns explojure.util-test
  (:require [explojure.util :refer :all]
            [clojure.test :refer :all]))

(deftest index-seq-test
  (testing "General Functionality"
    (is (= (index-seq []) {}) "handle zero-length sequence")
    (is (= (index-seq [1 2 3]) {1 [0] 2 [1] 3 [2]}) "handle non-zero-length sequence")))
