(ns peloton.bson 
  (:import java.util.regex.Pattern)
  (:import java.util.Date)
  (:import java.io.ByteArrayOutputStream)
  (:import java.io.ByteArrayInputStream)
  (:import java.util.UUID)
  (:import java.sql.Timestamp)
  (:import java.nio.charset.Charset)
  (:use peloton.util))

(defrecord Min [])
(defrecord Max [])
(defrecord JavaScript [^String js scope])
;(defrecord Timestamp [timestamp])
(defrecord MD5 [^bytes bytes])
(defrecord ObjectID [^bytes id])
(defrecord UserDefined [^bytes bytes])
(defrecord Function [^bytes bytes])
(defrecord DBPointer [^String a-ns ^bytes some-bytes])
(defrecord Undefined [])

(defonce error (Error. "parse error"))
(defonce undefined (Undefined. ))

(def bytes-type (type (byte-array [])))

(defn put-i32! 
  [^ByteArrayOutputStream bs
   ^long n]
  (.write bs (bit-and 0x0ff n))
  (.write bs (bit-and 0x0ff (bit-shift-right n 8)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 16)))
  (.write bs (bit-and 0x0ff (bit-shift-right n 24))))

(defn put-i64! 
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
   ^String key]
  (let [key0 (.getBytes key #^Charset UTF-8)]
    (.write bs key0 0 (count key0)))
  (.write bs 0))

(defn put-string! 
  [^ByteArrayOutputStream bs
   ^String s]
  (let [b (.getBytes s #^Charset UTF-8)]
    (put-i32! bs (+ 1 (count b)))
    (.write bs b 0 (count b))
    (.write bs 0)))

(defmacro put-pair! 
  [bs code a-key & body]
  `(let [key0# (if (keyword? ~a-key) (name ~a-key) ~a-key)]
     (.write #^ByteArrayOutputStream ~bs (int ~code))
     (put-cstring! ~bs key0#)
     ~@body))

(def pattern-flag-to-code {Pattern/CANON_EQ "u"
                           Pattern/CASE_INSENSITIVE "i"
                           Pattern/UNICODE_CASE "\\W"
                           Pattern/DOTALL ""
                           Pattern/UNIX_LINES ""
                           Pattern/LITERAL ""  
                           Pattern/MULTILINE "M"})

(defn encode-doc
  [doc]
  (let [bs (ByteArrayOutputStream. 4)]
    (.write bs 0)
    (.write bs 0)
    (.write bs 0)
    (.write bs 0)
    (doseq [[k v] doc] 

      (cond 
        (float? v) (put-pair! bs 
                              0x01 
                              k 
                              (put-i64! bs (Double/doubleToLongBits v)))
        (string? v) (put-pair! bs 
                               0x02 
                               k 
                               (let [b (.getBytes v #^Charset UTF-8)
                                     n (count b)]
                                 (put-i32! bs (inc n))
                                 (.write bs b 0 n)
                                 (.write bs 0)))
        (map? v) (put-pair! bs 
                            0x03 
                            k 
                            (let [b (encode-doc v)]
                              (.write bs b 0 (count b))))
        (sequential? v) (put-pair! bs 
                                   0x04 
                                   k 
                                   (let [a-doc (for [[i v] (map-indexed vector v)]
                                                 [(str i) v])
                                         doc-bytes (encode-doc a-doc)]
                                     (.write bs #^bytes doc-bytes 0 (count doc-bytes))))
        ; 0x05, 0x0 generic bytes
        (instance? bytes-type v) (put-pair! bs
                                            0x05 
                                            k
                                            (put-i32! bs (count v))
                                            (.write bs 0x00)
                                            (.write bs #^bytes v 0 (count v)))
        ; 0x05, 0x01 function
        (instance? Function v) (put-pair! bs
                                          0x05 
                                          k
                                          (put-i32! bs (count (:bytes v)))
                                          (.write bs 0x01)
                                          (.write bs #^bytes (:bytes v) 0 (count (:bytes v))))
        ; 0x05, 0x02 binary old
        ; 0x05, 0x03 UUID
        (instance? UUID v) (put-pair!
                             bs
                             0x05
                             k
                             (put-i32! bs 8)
                             (.write bs 0x03)
                             (put-i64! bs (.getLeastSignificantBits #^UUID v))
                             (put-i64! bs (.getMostSignificantBits #^UUID v)))
        ; 0x05, 0x05 MD5
        (instance? MD5 v) (put-pair! bs
                                     0x05
                                     k
                                     (put-i32! bs (count (:bytes v)))
                                     (.write bs 0x05)
                                     (.write bs (:bytes v) 0 (count (:bytes v))))
        ; 0x05, 0x80 UserDefined
        (instance? UserDefined v) (put-pair! bs
                                             0x05
                                             k
                                             (put-i32! bs (count (:bytes v)))
                                             (.write bs 0x80)
                                             (.write bs (:bytes v) 0 (count (:bytes v))))
        ; 0x06 - undefined
        (instance? Undefined v) (put-pair! bs 0x06 k)
        ; 0x07 - ObjectID
        (instance? ObjectID v) (put-pair! bs
                                          0x07 
                                          k 
                                          (.write bs (:bytes v) 12))
        ; 0x08 bool 
        (instance? Boolean v) (put-pair! bs 
                                         0x08 
                                         k
                                         (.write bs (if v 0x01 0x00)))
        ; 0x09 date
        (instance? Date v) (put-pair! bs
                                      0x09
                                      k
                                      (put-i64! bs (.getTime #^Date v)))
        ; 0x0A null
        (nil? v) (put-pair! bs 0x0a k)
        ; 0x0B regex
        (instance? Pattern v) (put-pair!
                                bs
                                0x0B 
                                k
                                (put-cstring! bs (.pattern v))
                                (let [flags (.flags v)
                                      s (apply str 
                                               (sort 
                                                 (for [[f s] pattern-flag-to-code] 
                                                   (if (bit-and f flags) s ""))))]
                                  (put-cstring! bs s)))

        ; 0x0C DBPointer
        ; 0x0D JavaScript
        (and (instance? JavaScript v)
             (nil? (:scope v))) (put-pair! bs 
                                           0x0D
                                           k
                                           (put-string! (:code v)))
        ; 0x0E Symbol
        (keyword? v) (put-pair! bs 
                                0x0E
                                k
                                (put-string! (name v)))
        ; 0x0F JavaScript w/ code
        (instance? JavaScript v) (put-pair! bs
                                            0x0F
                                            k
                                            (let [doc-bytes (encode-doc (:scope v))
                                                  d-len (count doc-bytes)
                                                  s-bytes (.getBytes #^String (:code v) #^Charset UTF-8)
                                                  s-len (count s-bytes)
                                                  n (+ 4 4 s-len 1 d-len)]
                                              (put-i32! bs n)
                                              (put-i32! bs (inc s-len))
                                              (.write bs s-bytes 0 s-len)
                                              (.write bs 0)
                                              (.write bs doc-bytes 0 d-len)))
        ; 0x10 int32 
        (integer? v) (put-pair! bs 
                                0x10
                                k
                                (put-i32! bs v))
        ; 0x11 timestamp
        (instance? Timestamp v) (put-pair! bs
                                           0x11
                                           k
                                           (put-i64! (.getTime #^Timestamp v)))
        ; 0x12 i64
        (instance? Long v) (put-pair! bs
                                      0x12
                                      k
                                      (put-i64! v))
        ; 0xff min
        (instance? Min) (put-pair! bs 0xFF k)
        ; 0x7f max
        (instance? Max) (put-pair! bs 0x7F k)))
    (.write bs 0)
    (let [^bytes a (.toByteArray bs)
          n (count a)]
      (aset-byte a 0 (bit-and 0xff n))
      (aset-byte a 1 (bit-and 0xff (bit-shift-right n 8)))
      (aset-byte a 2 (bit-and 0xff (bit-shift-right n 16)))
      (aset-byte a 3 (bit-and 0xff (bit-shift-right n 24)))
      a)))


(defn read-i32!
  ^long
  [^ByteArrayInputStream bs]
  (if (>= (.available bs) 4)
    (bit-or (.read bs) 
            (bit-shift-left (.read bs) 8)
            (bit-shift-left (.read bs) 16)
            (bit-shift-left (.read bs) 24)) 0))

(defn read-i64! 
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
          (= b 0x0) (String. (.toByteArray buf) #^Charset UTF-8)
          :else (let []
                  (.write buf b)
                  (recur)))))))

(defn read-string! 
  [^ByteArrayInputStream bs]
  (let [n (read-i32! bs)
        buf (ByteArrayOutputStream. (dec n))]
    ; the prefix is 
    (loop []
      (let [b (read-i8! bs)]
        (cond 
          (nil? b) nil
          (= b 0x0) (String. (.toByteArray buf) #^Charset UTF-8)
          :else (let []
                  (.write buf b)
                  (recur)))))))

(defn read-bytes! 
  ^bytes
  [^ByteArrayInputStream bs 
   ^long n]
  (let [buf (ByteArrayOutputStream. n)]
    (loop [n n]
      (when (and (> n 0) (> (.available bs) 0))
        (.write buf (.read bs))
        (recur (dec n))))
    (.toByteArray buf)))

(defn read-doc! 
  [^ByteArrayInputStream bs]
  (let [doc (transient {})
        doc-len (read-i32! bs)]
    (loop [etype (read-i8! bs)]
      (when (not (or (= etype 0) (nil? etype)))
        (let [a-name (read-cstring! bs)
              a-val (cond
                      (= etype 0x01) (safe nil (Double/longBitsToDouble (read-i64! bs)))
                      (= etype 0x02) (read-string! bs)
                      (= etype 0x03) (read-doc! bs)
                      (= etype 0x04) (map second (sort-by first (for [[k v] (read-doc! bs)] [(safe-int k) v])))
                      (= etype 0x05) (let [len (read-i32! bs)
                                           subtype (read-i8! bs)]
                                       (condp = subtype 
                                         0x00 (read-bytes! len)
                                         0x01 (Function. (read-bytes! len))
                                         0x02 (read-bytes! len)
                                         0x03 (let [lsb (read-i64! bs)
                                                    msb (read-i64! bs)]
                                                (UUID. msb lsb))
                                         0x05 (MD5. (read-bytes! len))
                                         0x80 (UserDefined. (read-bytes! len))))
                      (= etype 0x06) undefined
                      (= etype 0x07) (let [b (read-bytes! 12)]
                                       (when b (ObjectID. b)))
                      (= etype 0x08) (condp = (read-i8! bs)
                                       0x00 false
                                       0x01 true
                                       nil)
                      (= etype 0x09) (let [b (read-i64! bs)]
                                       (when (not (nil? b)) (Date. b)))
                      (= etype 0x0A) nil
                      (= etype 0x0B) (let [pat-s (read-cstring! bs)
                                           flag-s (read-cstring! bs)
                                           flags (apply bit-or
                                                        (for [[flag flag-key] pattern-flag-to-code]
                                                          (if (.contains flag-s flag-key) flag 0)))]
                                       (Pattern/compile pat-s flags))
                      (= etype 0x0C) (let [a-ns (read-string! bs)
                                           an-id (read-cstring! bs)]
                                       (when (and a-ns an-id)
                                         (DBPointer. a-ns an-id)))
                      (= etype 0x0D) (let [s (read-string! bs)]
                                       (when s
                                         (JavaScript. s nil)))
                      (= etype 0x0E) (let [s (read-string! bs)]
                                       (when s
                                         (keyword s)))
                      (= etype 0x0F) (let [s (read-string! bs)
                                           doc (read-doc! bs)]
                                       (when (and s doc)
                                         (JavaScript. s doc)))
                      (= etype 0x10) (read-i32! bs)
                      (= etype 0x11) (let [b (read-i64! bs)]
                                       (when (not (nil? b)) (Timestamp. b)))
                      (= etype 0x12) (read-i64! bs)
                      (= etype 0xFF) (Min.)
                      (= etype 0x7F) (Max.)
                      :else nil)]
          (assoc! doc (keyword a-name) a-val)
          (recur (read-i8! bs)))))
    (read-i8! bs)
    (persistent! doc)))

(defn decode-doc
  [^bytes in-doc-bytes]
  (let [bs (ByteArrayInputStream. in-doc-bytes)]
    (read-doc! bs)))

