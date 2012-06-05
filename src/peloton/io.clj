(ns peloton.io
  (:require [peloton.reactor :as reactor])
  (:import java.nio.ByteOrder)
  (:use peloton.util) 
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.ByteBuffer))

(def selection-key-flags {:connect? SelectionKey/OP_CONNECT 
                           :read? SelectionKey/OP_READ
                           :write? SelectionKey/OP_WRITE
                           :accept? SelectionKey/OP_ACCEPT})

(defn reg
  [socket-channel op f & fargs]
  (reactor/later 0
                 (with-stderr
                   (println "register:" (from-flag-bits op selection-key-flags) f fargs))
                 (let [g (fn [] 
                           (apply f fargs)
                           (.cancel reactor/selection-key))]
                   (reactor/register socket-channel op g))))

(defn on-readable 
  [^SocketChannel socket-channel f & fargs]
  (apply reg socket-channel SelectionKey/OP_READ f fargs))

(defn on-writable 
  [^SocketChannel socket-channel f & fargs]
  (apply reg socket-channel SelectionKey/OP_WRITE f fargs))

(defn on-connected
  [^SocketChannel socket-channel f & fargs]
  (with-stderr
    (println "wait for connect"))
  (apply reg socket-channel SelectionKey/OP_CONNECT f fargs))
  
(defn fill-buffer!
  "Fill up a byte buffer"
  [^SocketChannel socket-channel 
   ^ByteBuffer buffer 
   on-buffer]
  (with-stderr
    (println "Fill buffer" buffer on-buffer))
  (loop []
     (cond
       (= (.remaining buffer) 0) (on-buffer buffer)
       :else (let [amt (.read socket-channel buffer)] 
               (with-stderr 
                 (println "read" amt))
               (condp = amt
                 -1 (on-buffer buffer)
                 0 (on-readable socket-channel fill-buffer! socket-channel buffer on-buffer)
                 (recur))))))

;(defn read-n-bytes!
;  "Read up to n bytes"
;  [^SocketChannel socket-channel 
;   ^long n 
;   on-bytes]
;  (with-stderr 
;    (println "read n" n on-bytes))
;  (fill-buffer! 
;    socket-channel 
;    (ByteBuffer/allocate n) 
;    (fn [^ByteBuffer buffer] 
;      (cond 
;        (> (.remaining buffer) 0) (on-bytes nil)
;        :else (do
;                (on-bytes (.array buffer)))))))

(defn read-to-buf! 
  [ch ^long n on-buf]
  (with-stderr
    (println "reading buf" n on-buf))
  (fill-buffer! ch 
                (ByteBuffer/allocate n) 
                (fn [^ByteBuffer b]
                  ; constrain to n:
                  (cond 
                    (not (= (.remaining b) 0)) (on-buf nil)
                    :else (on-buf b)))))

(defn read-le-i32!
  [^SocketChannel ch
   on-int]
  (with-stderr
    (println "reading int" on-int))
  (read-to-buf! 
    ch
    4
    (fn [^ByteBuffer buffer] 
      (cond
        (nil? buffer) (on-int nil)
        :else (do 
                (.flip buffer)
                (.order buffer ByteOrder/LITTLE_ENDIAN)
                (on-int (.getInt buffer)))))))
