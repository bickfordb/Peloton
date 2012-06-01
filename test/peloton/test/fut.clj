(ns peloton.test.fut
  (:use [clojure.walk :only [macroexpand-all]])
  (:use peloton.fut)
  (:use clojure.test))

(deftest fut-test
         "test deliver/subscribe"
         (let [a-fut (fut)
               y (atom nil)]
           (is (nil? @y))
           (subscribe! a-fut (fn [x] (reset! y x)))
           (is (nil? @y))
           (deliver! a-fut "hello")
           (is (= @y "hello"))))

(deftest let-fut-test
  (let [a-fut (fut)
        a-fut2 (fut)
        x (atom nil)
        y (atom nil)
        z (atom nil)]
    (is (nil? @y))
    (let-fut
      [[a-val0] a-fut
       [a-val1] a-fut2
       quux 25]
      (reset! x quux)
      (reset! y a-val0)
      (reset! z a-val1))
    (is (nil? @x))
    (is (nil? @y))
    (is (nil? @z))
    (deliver! a-fut "hello there")
    (is (nil? @x))
    (is (nil? @y))
    (is (nil? @z))
    (deliver! a-fut2 "goodbye")
    (is (= 25 @x))
    (is (= "hello there" @y))
    (is (= "goodbye" @z))))

