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
  (apply reg socket-channel SelectionKey/OP_CONNECT f fargs))
  
(defn fill-buffer!
  "Fill up a byte buffer"
  [^SocketChannel socket-channel 
   ^ByteBuffer buffer 
   on-buffer]
  (loop []
     (cond
       (= (.remaining buffer) 0) (on-buffer buffer)
       :else (let [amt (.read socket-channel buffer)] 
               (condp = amt
                 -1 (on-buffer buffer)
                 0 (on-readable socket-channel fill-buffer! socket-channel buffer on-buffer)
                 (recur))))))

(defn read-to-buf! 
  [ch ^long n on-buf]
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
