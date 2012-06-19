(ns peloton.test.reactor
  (:use peloton.reactor)
  (:use clojure.test))

(deftest reactor-test
         (let [a (atom 0)]
           (is (= @a 0))
           (with-reactor 
             (later! 0
                     (swap! a inc)
                     (stop!)))
           (is (= @a 1))))
