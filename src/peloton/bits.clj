(ns peloton.bits
  (:import java.io.ByteArrayInputStream)
  (:import java.io.ByteArrayOutputStream)
  (:import java.nio.ByteBuffer)
  (:use peloton.util))

(defn put-le-i32! 
  [^ByteArrayOutputStream bs
   ^long n]
  (.write bs (bit-and 0x0ff n))
  (.write bs (bit-and 0x0ff (bit-shift-right n 8)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 16)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 24))))

(defn put-le-i64! 
  [^ByteArrayOutputStream bs
   ^long n]
  (.write bs (bit-and 0x0ff n))
  (.write bs (bit-and 0x0ff (bit-shift-right n 8)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 16)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 24)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 32)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 40)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 48)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 56))))

(defn put-cstring!
  [^ByteArrayOutputStream bs
   ^String a-str]
  (.write bs (encode-utf8 a-str))
  (.write bs 0))

(defn read-le-i32!
  ^long
  [^ByteArrayInputStream bs]
  (if (>= (.available bs) 4)
    (bit-or (.read bs) 
            (bit-shift-left (.read bs) 8)
            (bit-shift-left (.read bs) 16)
            (bit-shift-left (.read bs) 24)) 0))

(defn read-le-i64! 
  ^long
  [^ByteArrayInputStream bs]
  (if (>= (.available bs) 8)
    (bit-or (.read bs) 
            (bit-shift-left (.read bs) 8)
            (bit-shift-left (.read bs) 16)
            (bit-shift-left (.read bs) 24) 
            (bit-shift-left (.read bs) 32)
            (bit-shift-left (.read bs) 40)
            (bit-shift-left (.read bs) 48)
            (bit-shift-left (.read bs) 56)) 0))

(defn read-i8! 
  ^Integer
  [^ByteArrayInputStream bs]
  (when (>= (.available bs) 1)
    (.read bs)))

(defn read-cstring! 
  ^String
  [^ByteArrayInputStream bs]
  (let [buf (ByteArrayOutputStream. 1)]
    (loop []
      (let [b (read-i8! bs)]
        (cond 
          (nil? b) nil
          (= b 0x0) (decode-utf8 (.toByteArray buf))
          :else (let []
                  (.write buf b)
                  (recur)))))))

(defn slice-buffer
  ^bytes [^ByteBuffer b
   offset
   len]
  (java.util.Arrays/copyOfRange (.array b) (long offset) (long (+ offset len))))

(defn slice-remaining-buffer
  ^bytes [^ByteBuffer b]
  (slice-buffer b (.position b) (long (- (.limit b) (.position b)))))

(defn remove-bit
  [bits n]
  (bit-and bits (bit-flip n)))

