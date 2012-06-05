(ns peloton.bson 
  (:import java.util.regex.Pattern)
  (:import java.util.Date)
  (:import java.nio.ByteBuffer)
  (:import java.io.ByteArrayOutputStream)
  (:import java.io.ByteArrayInputStream)
  (:import java.util.UUID)
  (:import java.sql.Timestamp)
  (:import java.nio.charset.Charset)
  (:use peloton.bits)
  (:use peloton.util))

(defonce machine-id #^long (-> (java.util.Random.) (.nextLong)))
(defonce last-object-id (atom 1))

(defrecord Min [])
(defrecord Max [])
(defrecord JavaScript [^String js scope])

(defrecord MD5 [^bytes bytes])

(deftype ObjectID [^bytes id]
   Object 
    (toString [this] (format "(ObjectID. %s)" (encode-hex id))) 
    (hashCode [this] (+ (hash ObjectID) (apply + (seq id))))
    (equals [this other] 
      (cond 
        (not (instance? ObjectID other)) false
        (and (nil? id) (nil? (.id #^ObjectID other))) true
        (nil? id) false
        (nil? (.id #^ObjectID other)) false
        :else (java.util.Arrays/equals id (.id #^ObjectID other)))))

(defrecord UserDefined [^bytes bytes])
(defrecord Function [^bytes bytes])
(defrecord DBPointer [^String a-ns ^bytes some-bytes])
(defrecord Undefined [])

(defonce error (Error. "parse error"))
(defonce undefined (Undefined. ))

(def bytes-type (type (byte-array [])))

(defn put-string! 
  [^ByteArrayOutputStream bs
   ^String s]
  (let [b (.getBytes s #^Charset UTF-8)]
    (put-le-i32! bs (+ 1 (count b)))
    (.write bs b 0 (count b))
    (.write bs 0)))

(defmacro put-pair! 
  [bs code a-key & body]
  `(let [k# ~a-key
         key0# (if (keyword? k#) (name k#) k#)]
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
                              (put-le-i64! bs (Double/doubleToLongBits v)))
        (string? v) (put-pair! bs 
                               0x02 
                               k 
                               (let [b (.getBytes v #^Charset UTF-8)]
                                 (put-le-i32! bs (inc (count b)))
                                 (.write bs b)
                                 (.write bs (byte 0))))
        
        ; 0x05, 0x0 generic bytes
        (instance? bytes-type v) (put-pair! bs
                                            0x05 
                                            k
                                            (put-le-i32! bs (count v))
                                            (.write bs 0x00)
                                            (.write bs #^bytes v 0 (count v)))
        ; 0x05, 0x01 function
        (instance? Function v) (put-pair! bs
                                          0x05 
                                          k
                                          (put-le-i32! bs (count (:bytes v)))
                                          (.write bs 0x01)
                                          (.write bs #^bytes (:bytes v) 0 (count (:bytes v))))
        ; 0x05, 0x02 binary old
        ; 0x05, 0x03 UUID
        (instance? UUID v) (put-pair!
                             bs
                             0x05
                             k
                             (put-le-i32! bs 8)
                             (.write bs 0x03)
                             (put-le-i64! bs (.getLeastSignificantBits #^UUID v))
                             (put-le-i64! bs (.getMostSignificantBits #^UUID v)))
        ; 0x05, 0x05 MD5
        (instance? MD5 v) (put-pair! bs
                                     0x05
                                     k
                                     (put-le-i32! bs (count (:bytes v)))
                                     (.write bs 0x05)
                                     (.write bs (:bytes v) 0 (count (:bytes v))))
        ; 0x05, 0x80 UserDefined
        (instance? UserDefined v) (put-pair! bs
                                             0x05
                                             k
                                             (put-le-i32! bs (count (:bytes v)))
                                             (.write bs 0x80)
                                             (.write bs (.bytes #^UserDefined v) 0 (count (:bytes v))))
        ; 0x06 - undefined
        (instance? Undefined v) (put-pair! bs 0x06 k)
        ; 0x07 - ObjectID
        (instance? ObjectID v) (put-pair! bs
                                          0x07 
                                          k 
                                          (.write bs (.id #^ObjectID v) 0 12))
        ; 0x08 bool 
        (instance? Boolean v) (put-pair! bs 
                                         0x08 
                                         k
                                         (.write bs (if v 0x01 0x00)))
        ; 0x09 date
        (instance? Date v) (put-pair! bs
                                      0x09
                                      k
                                      (put-le-i64! bs (.getTime #^Date v)))
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
                                              (put-le-i32! bs n)
                                              (put-le-i32! bs (inc s-len))
                                              (.write bs s-bytes 0 s-len)
                                              (.write bs 0)
                                              (.write bs doc-bytes 0 d-len)))
        ; 0x10 int32 
        (integer? v) (put-pair! bs 
                                0x10
                                k
                                (put-le-i32! bs v))
        ; 0x11 timestamp
        (instance? Timestamp v) (put-pair! bs
                                           0x11
                                           k
                                           (put-le-i64! (.getTime #^Timestamp v)))
        ; 0x12 i64
        (instance? Long v) (put-pair! bs
                                      0x12
                                      k
                                      (put-le-i64! v))
        ; 0xff min
        (instance? Min) (put-pair! bs 0xFF k)
        ; 0x7f max
        (instance? Max) (put-pair! bs 0x7F k)
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
                                     (.write bs #^bytes doc-bytes 0 (count doc-bytes))))))
    (.write bs 0)
    (let [^bytes a (.toByteArray bs)
          n (count a)]
      (aset-byte a 0 (bit-and 0xff n))
      (aset-byte a 1 (bit-and 0xff (bit-shift-right n 8)))
      (aset-byte a 2 (bit-and 0xff (bit-shift-right n 16)))
      (aset-byte a 3 (bit-and 0xff (bit-shift-right n 24)))
      a)))



(defn read-string! 
  [^ByteArrayInputStream bs]
  (let [n (read-le-i32! bs)
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
   ;^long 
   n]
  (let [buf (ByteArrayOutputStream. n)]
    (loop [i n]
      (when (and (> i 0) (> (.available bs) 0))
        (.write buf (.read bs))
        (recur (dec i))))
    (cond 
      (= (.size buf) n) (.toByteArray buf)
      :else (throw (Exception. (format "expecting %d bytes" n))))))

(defn check
  [x]
  (cond
    (nil? x) (throw (Exception. "unexpected nil"))
    :else x))

(defmacro check0
  [x]
  `(let [v# ~x]
    (cond 
      (nil? v#) (throw (Exception. "unexpected nil"))
      :else v#)))

(defn read-doc! 
  [^ByteArrayInputStream bs]
  (let [doc (transient {})
        doc-len (read-le-i32! bs)]
    (loop [etype (read-i8! bs)]
      (when (not (or (= etype 0) (nil? etype)))
        (let [a-name (read-cstring! bs)
              a-val (cond
                      (= etype 0x01) (Double/longBitsToDouble (read-le-i64! bs))
                      (= etype 0x02) (check0 (read-string! bs))
                      (= etype 0x03) (check0 (read-doc! bs))
                      (= etype 0x04) (map second (sort-by first (for [[k v] (read-doc! bs)] [(Integer/parseInt (name k)) v])))
                      (= etype 0x05) (let [len (read-le-i32! bs)
                                           subtype (read-i8! bs)]
                                       (condp = subtype 
                                         0x00 (check0 (read-bytes! bs len))
                                         0x01 (Function. (check0 (read-bytes! bs len)))
                                         0x02 (check0 (read-bytes! bs len))
                                         0x03 (let [lsb (read-le-i64! bs)
                                                    msb (read-le-i64! bs)]
                                                (UUID. msb lsb))
                                         0x05 (MD5. (read-bytes! bs len))
                                         0x80 (UserDefined. (check0 (read-bytes! bs len)))
                                         :else (throw (Exception. "unexpected subtype"))))
                      (= etype 0x06) undefined
                      (= etype 0x07) (let [b (check0 (read-bytes! bs 12))]
                                       (when b (ObjectID. b)))
                      (= etype 0x08) (condp = (read-i8! bs)
                                       0x00 false
                                       0x01 true
                                       nil)
                      (= etype 0x09) (let [b (read-le-i64! bs)]
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
                      (= etype 0x10) (read-le-i32! bs)
                      (= etype 0x11) (let [b (read-le-i64! bs)]
                                       (when (not (nil? b)) (Timestamp. b)))
                      (= etype 0x12) (read-le-i64! bs)
                      (= etype 0xFF) (Min.)
                      (= etype 0x7F) (Max.)
                      :else (throw (Exception. (format "unexpected etype 0x%X" (int etype)))))]
          (assoc! doc (keyword a-name) a-val)
          (recur (read-i8! bs)))))
    ;(read-i8! bs)
    (persistent! doc)))

(defn decode-doc
  [^bytes in-doc-bytes]
  (let [bs (ByteArrayInputStream. in-doc-bytes)]
    (read-doc! bs)))

(defn create-object-id
  []
  (let [bs (ByteBuffer/allocate 12)
        t0 (System/currentTimeMillis)
        m (long machine-id)
        i (long (swap! last-object-id inc))]
    (.putInt bs (bit-and (long (/ t0 10000.0)) 0x7fffffff))
    (.putInt bs (int (bit-and machine-id 0x7fffffff)))
    (.putInt bs (bit-and i 0x7fffffff))
    (ObjectID. (.array bs))))

(defn add-object-id 
  [doc]
  (if (contains? doc :_id) 
    doc
    (assoc doc :_id (create-object-id))))

