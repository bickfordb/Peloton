(ns peloton.io
  (:import java.io.ByteArrayOutputStream)
  (:import java.nio.ByteBuffer)
  (:import java.nio.ByteOrder)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.charset.Charset)
  (:require [peloton.reactor :as reactor])
  (:use peloton.util) 
  )
(set! *warn-on-reflection* true)
  
(defn fill-buffer!
  "Fill up a byte buffer"
  [^SocketChannel socket-channel 
   ^ByteBuffer buffer 
   on-buffer
   & args]
  (loop []
     (cond
       (= (.remaining buffer) 0) (on-buffer buffer)
       :else (let [amt (.read socket-channel buffer)] 
               (condp = amt
                 -1 (apply on-buffer (concat args [buffer]))
                 0 (apply reactor/on-readable-once! socket-channel fill-buffer! socket-channel buffer on-buffer args)
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

; array which matches the end of headers

