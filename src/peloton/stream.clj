(ns peloton.stream
  (:use peloton.util)
  (:require [peloton.fut :as fut]))

(definterface IStream
  (bind_BANG_ [^clojure.lang.IFn f])
  (close_BANG_ [])
  (^boolean eof_QMARK_ [])
  (invoke [x])
  (unbind_BANG_ [f]))

(deftype Stream 
  [^clojure.lang.PersistentQueue ^{:volatile-mutable true} buffer 
   ^boolean ^{:volatile-mutable true} closed?
   ^clojure.lang.IFn ^{:volatile-mutable true} target]
  IStream
  (eof? [this] (and (empty? buffer) closed?))
  (bind! [this t]
    (set! target t)
    (while (and 
            target 
            (not (empty? buffer)))
      (t (first buffer))
      (set! buffer (rest buffer)))
    (when (and target closed?)
      (target nil)) ; send eof message
    nil)
  (unbind! [this t]
    (set! target t))
  (close! [this] 
    (set! closed? (boolean true))
    (when target (target nil)))
  clojure.lang.IFn
  (invoke [this a-val] 
    (if target
      (target a-val)
      (set! buffer (conj buffer a-val)))))

(defn stream
  [] 
  (Stream. (clojure.lang.PersistentQueue/EMPTY) false nil))

(defn stream?  
  "Check to see if s is a stream"
  [s] 
  (instance? peloton.stream.IStream s))

(defmacro do-stream
  "Run a body block in a stream with a binding
  "
  [bindings & body]
  (assert (= (count bindings) 2) "expecting 2 bindings")
  (let [[h t] bindings]
    `(let [t# ~t
           f# (fut/fut)
           g# (fn [~h] 
                (if (.eof? t#)
                  (f# true)
                  (do ~@body)))]
       (cond
         (stream? t#) (.bind! #^peloton.stream.IStream t# g#)
         :else (g# t#))
       f#)))
  
