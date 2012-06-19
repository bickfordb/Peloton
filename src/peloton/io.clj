(ns peloton.io
  (:import java.io.ByteArrayOutputStream)
  (:import java.nio.ByteBuffer)
  (:import java.nio.ByteOrder)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.charset.Charset)
  (:require [peloton.reactor :as reactor])
  (:use peloton.fut)
  (:use peloton.util) 
  )
(set! *warn-on-reflection* true)
  
(defn fill-buffer!
  "Fill up a byte buffer"
  [reactor
   ^SocketChannel socket-channel 
   ^ByteBuffer buffer 
   on-buffer
   & args]
  (loop []
     (cond
       (= (.remaining buffer) 0) (on-buffer buffer)
       :else (let [amt (.read socket-channel buffer)] 
               (condp = amt
                 -1 (apply on-buffer (concat args [buffer]))
                 0 (apply reactor/on-reactor-readable-once! reactor socket-channel fill-buffer! reactor socket-channel buffer on-buffer args)
                 (recur))))))

(defn read-to-buf! 
  [^SocketChannel ch 
   n 
   on-buf 
   & args]
  (fill-buffer! 
         ch 
         (ByteBuffer/allocate n) 
         (fn [^ByteBuffer b]
           ; constrain to n:
           (cond 
             (not (= (.remaining b) 0)) (apply on-buf (concat args [nil]))
             :else (apply on-buf (concat args [b]))))))

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

(defn read-line!' 
  [a-reactor ^SocketChannel chan ^ByteArrayOutputStream some-bytes f]
  (let [bb (ByteBuffer/allocate 1)]
    (loop []
      (condp = (.read chan bb) 
        -1 (f nil)
        0 (peloton.reactor/on-reactor-readable-once! a-reactor chan read-line!' a-reactor chan some-bytes f)
        (do 
          (.flip bb)
          (let [a-byte (.get bb)]
            (.compact bb)
            (.write some-bytes #^int (int a-byte))
            (if (== a-byte 10) ; nl
              (f (.toByteArray some-bytes))
              (recur))))))))

(defn read-line! 
  [a-reactor chan]
  (let [f (fut)]
    (read-line!' a-reactor chan (ByteArrayOutputStream. ) f)
    f))
; array which matches the end of headers

