(ns peloton.test.fut
  (:use [clojure.walk :only [macroexpand-all]])
  (:use peloton.fut)
  (:use peloton.cell)
  (:use clojure.test))

(deftest fut-test
         "test deliver/subscribe"
         (let [a-fut (fut)
               y (atom nil)]
           (is (nil? @y))
           (bind! a-fut (fn [x] (reset! y x)))
           (is (nil? @y))
           (a-fut "hello")
           (is (= @y "hello"))))

(deftest dofut-test
  (let [a-fut (fut)
       a-fut2 (fut)
        x (atom nil)
        y (atom nil)
        z (atom nil)]
    (is (nil? @y))
    (dofut
    [a-val0 a-fut
       a-val1 a-fut2
       quux 25]
      (reset! x quux)
      (reset! y a-val0)
      (reset! z a-val1))
    (is (nil? @x))
    (is (nil? @y))
    (is (nil? @z))
    (a-fut "hello there")
    (is (nil? @x))
    (is (nil? @y))
    (is (nil? @z))
    (a-fut2 "goodbye")
    (is (= 25 @x))
    (is (= "hello there" @y))
    (is (= "goodbye" @z))))

(deftest dofut-sequence
         (let [f0 (fut)
               f1 (fut)
               c0 (atom nil)
               c1 (atom nil)]
           (dofut [v0 f0 
                   v1 (do 
                        (reset! c0 :fired0)
                        f1)]
                  (reset! c1 :fired1))
           (is (nil? @c0 ))
           (is (nil? @c1 ))
           (f0 "v0")
           (is (nil? @c1 ))
           (is (= @c0 :fired0))
           (f1 "v1")
           (is (= @c0 :fired0))
           (is (= @c1 :fired1))))

(deftest alg-test-0
         (>? 
           (let [v0 (atom nil)
                 f0 (fut)]
             (reset! v0 ?f0)
             (is (= @v0 nil))
             (f0 1)
             (is (= @v0 1)))))

;(deftest alg-test-1
;         (let [v0 (atom nil)
;               f0 (fut)
;               f1 (fut)
;               f2 (fut)]
;           (>? 
;              (let [q0 (+ ?f0 ?f1)]
;                (println "q0: " ?q0)))))

(deftest dofut-test-0
         (let [a (fut)
               b (dofut [x a]
                        (+ 1 x))
               cell (atom nil)]
           (dofut [y b]
                  (reset! cell y))
           (is (= @cell nil))
           (a 1)
           (is (= @cell 2))))
          


 
