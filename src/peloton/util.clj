(ns peloton.util
  (:import java.io.ByteArrayInputStream)
  (:import java.io.ByteArrayOutputStream)
  (:import java.nio.ByteBuffer)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.charset.Charset)
  (:require clojure.string))

(defonce NEWLINE #^int (int 10))
(defonce ISO-8859-1 #^Charset (.get (Charset/availableCharsets) "ISO-8859-1"))
(defonce UTF-8 #^Charset (.get (Charset/availableCharsets) "ISO-8859-1"))

(defmacro safe
  "Execute 'body' and return a 'default' if 'body' raises any Exception"
  [default & body]
  `(try
     ~@body
     (catch Exception e# ~default)))

(defn not-nil? [x] (not (nil? x)))

(defn safe-int
  "Convert a string to an integer or nil"
  [^String s]
  (try
    (when (and (not (nil? s)) (> (count s) 0))
      (Integer. s))
    (catch Exception e nil)))

(defonce empty-bytes 
  ;"Get an empty array"
  (bytes (into-array Byte/TYPE [])))

(defn dump-buffer
  "Copy a byte buffer to a string, escaping newlines"
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

(defn quote-plus 
  [^String s] 
  (if s (java.net.URLEncoder/encode s) ""))  

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
                      (when (not (= k "")) [(keyword k) v]))))))

(defn re-find0 
  "A version of re-find that consistently returns a sequence instead of sometimes returning a string or nil."
  [& xs]
  (let [ret (apply re-find xs)]
    (cond
      (nil? ret) []
      (string? ret) [ret]
      :else ret)))

(defn to-flag-bits 
  "Convert a mapping of flags (keyword -> boolean) and a flag definition mapping (keyword -> number) to a number. 
  "
  ^long
  [mapping flag-bit-def]
  (apply bit-or 0 (for [[flag-kw flag-bit] flag-bit-def]
                    (if (get mapping flag-kw false) flag-bit 0))))

(defn from-flag-bits 
  "Convert a flag bits (encoded in a number) into a mapping of {flagi bool}"
  [^long flag-bits flag-bit-def]
  (apply assoc {} (apply concat
                         (for [[flag-kw flag-bit] flag-bit-def]
                           [flag-kw (> (bit-and flag-bit flag-bits) 0)]))))

(defmacro with-stderr 
  "Run body using stderr

  e.g. To print \"hello\" to stderr:
     (with-stderr
        (println \"hello\"))
  "
  [ & body] 
  `(binding [*out* *err*] ~@body))

(defn nib 
  "Takes all arguments and returns nil.

  This is useful as a callback argument where you don't care about the callback result.
  "
  [& xs] nil)

(defn encode-hex
  "Encode a sequence of numbers (byte-like values) to hex"
  [m]
  (apply str (for [i m] 
               (format "%x" i))))

(defn num-processors 
  "Get the number of processors"
  []
  (-> (Runtime/getRuntime) (.availableProcessors)))

(defmacro spread 
  "Execute body in N (processors) threads and return the result of each execution in a sequence."
  [& body]
  `(let [n# (num-processors)
         ret# (atom [])
         f# #(swap! ret# conj (do ~@body))
         threads# (doall (for [i# (range n#)] (Thread. f#)))]
     (doseq [^Thread t# threads#]
       (.setDaemon t# true)
       (.start t#))
     (doseq [^Thread t# threads#]
       (.join t#))
     @ret#))

(defmacro ignore-and-print
  [& body]
  `(try
     ~@body
     (catch Exception e# (.printStackTrace e#))))

(defmacro default 
  [x a-default]
  `(let [x# ~x]
    (if (nil? x#) ~a-default x#)))


(defn encode-utf8
  ^bytes [^String s]
  (when s
    (.getBytes s #^Charset UTF-8)))

(defn decode-utf8
  ^String [^bytes bs]
  (when bs 
    (String. bs #^Charset UTF-8)))
