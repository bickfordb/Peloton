(ns peloton.test.util
  (:use peloton.util)
  (:use clojure.test))

(deftest is-spread-working
    (let [result (spread (+ 1 1))]
      (is (>= (count result) 1))
      (is (= (apply + result) 
             (* 2 (num-processors))))))

