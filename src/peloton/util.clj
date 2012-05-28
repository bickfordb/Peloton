(ns peloton.util
  (:import java.io.ByteArrayOutputStream)
  (:require clojure.string)
  (:import java.nio.charset.Charset)
  (:import java.nio.ByteBuffer))

(defonce NEWLINE #^int (int 10))
(defonce ISO-8859-1 #^Charset (.get (Charset/availableCharsets) "ISO-8859-1"))
(defonce UTF-8 #^Charset (.get (Charset/availableCharsets) "ISO-8859-1"))

(defmacro safe
  [default & body]
  `(try 
     ~@body
     (catch Exception e# ~default)))

(defn not-nil? [x] (not (nil? x)))

(defn safe-int 
  [^String s] 
  (try
    (when (and (not (nil? s)) (> (count s) 0))
      (Integer. s))
    (catch Exception e nil)))

(definterface ILineParser 
  (^String put [^java.nio.ByteBuffer buffer]))

(deftype LineParser 
  [^ByteArrayOutputStream bs]
  ILineParser
  (put [this buffer]
    (loop []
      (when (> (.remaining buffer) 0)
        (let [a-char (int (.get buffer))]
          (.write bs a-char)
          (if (== a-char NEWLINE)
            (let [ret (String. (.toByteArray bs) #^Charset ISO-8859-1)]
              (.reset bs)
              ret)
            (recur)))))))

(defn line-parser 
  []
  (LineParser. (ByteArrayOutputStream. )))

(defonce empty-bytes (bytes (into-array Byte/TYPE [])))

(defn dump-buffer 
  [^ByteBuffer b]
  (let [ret (String. (java.util.Arrays/copyOfRange (.array b) (.position b) (.limit b)) ISO-8859-1)
        ret0 (clojure.string/escape ret {\newline "\\n" \return "\\r"})]
    ret0))

