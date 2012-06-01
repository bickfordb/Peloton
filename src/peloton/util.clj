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

(defmacro until
  "Keep executing body until it returns a non-nil result"
  [& body]
  `(let [f# (fn [] ~@body)]
    (loop [g# (f#)]
      (if (not-nil? g#)
        g#
        (recur (f#))))))

(def amp-pat #"[&]")
(def eq-pat #"=")

(defn parse-qs
  "Parse query string / form URL encoded data"
  [s]
  (cond
    (nil? s) []
    :else (filter #(not (nil? %)) 
                  (for [p (clojure.string/split s amp-pat)]
                    (let [[l r] (clojure.string/split p eq-pat 2)
                          k (if l (java.net.URLDecoder/decode l) "")
                          v (if r (java.net.URLDecoder/decode r) "")]
                      (when (not (= k "")) [k v]))))))

(defn re-find0 
  "A version of re-find that consistently returns the same type"
  [& xs]
  (let [ret (apply re-find xs)]
    (cond
      (nil? ret) []
      (string? ret) [ret]
      :else ret)))

(defn to-flag-bits 
  ^long
  [mapping flag-bit-def]
  (apply bit-or (for [[flag-kw flag-bit] flag-bit-def]
                  (if (get mapping flag-kw false) flag-bit 0))))

(defn from-flag-bits 
  "Convert a flag bits (encoded in a number) into a mapping of {flagi bool}"
  [^long flag-bits flag-bit-def]
  (apply assoc {} (apply concat
                         (for [[flag-kw flag-bit] flag-bit-def]
                           [flag-kw (> (bit-and flag-bit flag-bits) 0)]))))


