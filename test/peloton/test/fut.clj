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

(deftest dofut-test
  (let [a-fut (fut)
        a-fut2 (fut)
        x (atom nil)
        y (atom nil)
        z (atom nil)]
    (is (nil? @y))
    (dofut
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

(deftest dofut-test2
         (let [f0 (fut)
               f1 (fut)
               c0 (atom nil)
               c1 (atom nil)]
           (dofut [[v0] (f0) 
                     [v1] (do 
                          (reset! c0 :fired0)
                          f1)]
                    (reset! c1 :fired1))
           (is (nil? @c0 ))
           (is (nil? @c1 ))
           (deliver! f0 "v0")
           (is (nil? @c1 ))
           (is (= @c0 :fired0))
           (deliver! f1 "v1")
           (is (= @c0 :fired0))
           (is (= @c1 :fired1))
          ))

