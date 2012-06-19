(ns peloton.http
  (:import clojure.lang.PersistentQueue)
  (:import java.nio.channels.spi.SelectorProvider)
  (:import java.nio.ByteBuffer)
  (:import java.nio.channels.SocketChannel)
  (:import java.io.ByteArrayOutputStream)
  (:require peloton.reactor)
  (:require peloton.io)
  (:use peloton.util) 
  (:use peloton.fut))

(def header-pat #"^\s*(\S+)\s*[:]\s*(.+)\s*$")
(def response-line-pat #"^\s*(\S+)\s+(\d+)\s+(.+)\s*$") ; protocol, status, message

(def empty-headers PersistentQueue/EMPTY)

(defn get-header
  ^String [headers #^String header-name] 
  (loop [headers headers]
    (when (not (empty? headers))
      (let [[[^String k v] & t] headers]
        (if (.equalsIgnoreCase header-name k) 
          v 
          (recur t)))))) 

(defn delete-header
  [headers ^String a-name]
  (remove #(.equalsIgnoreCase a-name #^String (first %)) headers))

(defn add-header
  [headers #^String a-name a-value]
  (conj headers [a-name a-value]))

(defn set-header
  [headers #^String a-name a-value]
  (add-header (delete-header headers a-name) a-name a-value))

(defn add-header-if-missing
  [headers #^String a-name a-value]
  (cond 
    (nil? (get-header headers a-name)) (add-header headers a-name a-value)
    :else headers))


(def absolute-url-pat 
  #"(?x) # comments 
    ^(?:(\w+)[:]//)? # protocol
    ([^?/:\#;]+)?  # host
    (?:[:](\d+))? # port
    (/[^\#?;]+)? # path
    (?:;([^?\#]*))? # argument
    (?:[?]([^\#]*))? # query
    (?:\#(.*))? # fragment
   $") 

(defn parse-url 
  [url] 
  (let [m (re-find absolute-url-pat url)
        [_ protocol host port path arg query fragment] m]
    (when m {:protocol protocol
             :host host
             :port (when port (Integer. port))
             :arg arg
             :path path
             :query query
             :fragment fragment})))

; HTTP client

(defn connect!
  [a-reactor host port]
  (when host
    (let [f (fut)
          addr (java.net.InetSocketAddress. host (if port (int port) 80))
          chan (.openSocketChannel (.provider (peloton.reactor/selector a-reactor)))]
      (.configureBlocking chan false)
      (peloton.reactor/on-reactor-connectable-once! 
        a-reactor   
        chan 
        #(do 
           (.finishConnect chan)
           (f chan)))
      (.connect chan addr)
      f)))

(defprotocol IBytes
  (put-bytes! [bs ^bytes b])
  (put-string! [bs ^String s]))

(extend-protocol IBytes
  ByteArrayOutputStream
  (put-bytes! [self b] (.write self b))
  (put-string! [self s]
    (.write self (.getBytes s))))

(defn encode-params
  [params]
  (when (and params (> (count params) 0)) 
    (clojure.string/join 
      "&" 
      (for [[k v] params]
        (str (quote-plus (name k)) "=" (quote-plus v))))))

(defn encode-request 
  [request-opts]
  (let [{:keys [method path host port query params body headers protocol protocol-version]
         :or {method "GET"
              host "127.0.0.1"
              port 80
              body nil
              headers empty-headers
              protocol-version "1.1"
              protocol "http"}} request-opts
          ^String encoded-params (encode-params params)  
          method' (if (keyword? method) (name method) method)
          path' (if path path "/")
          query' (cond 
                    (and encoded-params 
                         (not (= method "POST"))) (if query (str query "&" encoded-params) encoded-params)
                  :else query)
          ^bytes body' (cond 
                         (and (= method "POST") 
                              encoded-params) (.getBytes encoded-params)
                         (string? body) (.getBytes #^String body)
                         :else body)
          headers' (-> headers
                    (add-header-if-missing "Host" host)
                    (add-header-if-missing "Content-Length" (count body'))
                    (add-header-if-missing "User-Agent" "peloton/0"))
          uri (if query' (format "%s?%s" path' query') path')
          request-line (format "%s %s %s/%s\r\n" method uri (.toUpperCase #^String protocol) protocol-version)
          buffer (ByteArrayOutputStream. )]
      (put-string! buffer request-line)
      (doseq [[k v] headers]
        (put-string! buffer (str k ": " v "\r\n")))
      (put-string! buffer "\r\n")
      (when body' 
        (put-bytes! buffer body'))
      (.toByteArray buffer)))

(defn write-buffer!
  [a-reactor ^SocketChannel chan ^ByteBuffer buf a-fut]
  (loop []
    (cond 
      (or (not a-reactor) (not chan) (not buf)) (a-fut false)
      (> (.remaining buf) 0) (let [n (.write chan buf)]
                               (cond 
                                 (= n 0) (peloton.reactor/on-reactor-writable-once! a-reactor chan write-buffer! a-reactor chan buf a-fut)
                                 (< n 0) (a-fut false)
                                 :else (recur)))
      :else (a-fut true))))

(defn write-request! 
  [a-reactor chan opts]
  (let [^bytes req-bytes (encode-request opts)
        buf (ByteBuffer/wrap req-bytes)
        f (fut)]
    (write-buffer! a-reactor chan buf f)
    f))

(defn read-head!'
  [a-reactor ^SocketChannel chan a-byte-array line-len f]
  (let [bb (ByteBuffer/allocate 1)]
    (loop [line-len line-len]
      (let [n (.read chan bb)]
        (cond
          (< n 0) (f nil)
          (= n 0) (peloton.reactor/on-reactor-readable-once! a-reactor chan read-head!' a-reactor chan a-byte-array line-len f)
          :else (do
                  (.flip bb)
                  (let [a-byte (.get bb)
                        is-nl? (== a-byte 10)
                        empty-line (and is-nl? (<= line-len 1))]
                    (.compact bb)
                    (.write a-byte-array #^int (int a-byte))
                    (cond 
                      empty-line (f (String. (.toByteArray a-byte-array)))
                      is-nl? (recur 0)
                      :else (recur (inc line-len))))))))))

(defn read-head!
  [a-reactor chan] 
  (let [f (fut)]
    (read-head!' a-reactor chan (ByteArrayOutputStream.) 0 f)
    f))
          
(defn parse-response-header 
  [^String s]
  (let [lines (.split s "\r\n")
        [response-line & header-lines] lines
        [_ protocol status message] (re-find response-line-pat response-line)
        status' (safe-int status)
        headers (remove empty? (map #(rest (re-find header-pat %)) header-lines))
        headers' (reduce conj PersistentQueue/EMPTY headers)]
    {:protocol protocol 
     :status status'
     :message message
     :headers headers'}))

(defn read-bytes!
  [a-reactor chan n] 
  (let [f (fut)
        buf (ByteBuffer/allocate n)
        g #(f (if % (.array buf) nil))]
    (peloton.io/fill-buffer! a-reactor chan buf g)
    f))

(defn read-chunk-len! 
  [a-reactor chan]
  (dofut [^bytes line (peloton.io/read-line! a-reactor chan)
         ^String line-s (.trim (String. line))]
         (when (and line-s (not (empty? line-s)))
           (Integer/parseInt line-s 16))))

(defn read-chunked-body!' 
  [a-reactor 
   chan 
   ^ByteArrayOutputStream some-bytes]
  (dofut [chunk-len (read-chunk-len! a-reactor chan)
          ok? (cond 
                (> chunk-len 0) (dofut [a-chunk (read-bytes! a-reactor chan chunk-len)
                                        _ (.write some-bytes a-chunk)
                                        _ (peloton.io/read-line! a-reactor chan)
                                        ; It seems like this recursion might explode the stack on a large number of small chunks
                                        rest? (read-chunked-body!' a-reactor chan some-bytes)]
                                       rest?)
                (= chunk-len 0) (dofut [line? (peloton.io/read-line! a-reactor chan)] true)
                :else false)]
         ok?))

(defn read-chunked-body! 
  [a-reactor chan]
  (let [bs (ByteArrayOutputStream.)]
    (dofut [ok? (read-chunked-body!' a-reactor chan bs)]
           (when ok? 
             (.toByteArray bs)))))

(defn read-response!  
  [a-reactor chan] 
  (dofut [header-lines (read-head! a-reactor chan)
          response-head (when header-lines (parse-response-header header-lines))
          {:keys [headers] :or {headers empty-headers}} response-head
          chunked? (= (get-header headers "Transfer-Encoding") "chunked")
          content-length (-> (get-header headers "Content-Length") safe-int)
          content-length' (max (default content-length 0) 0)
          body (if (not chunked?) 
                (read-bytes! a-reactor chan content-length')
                (read-chunked-body! a-reactor chan))]
      (assoc response-head 
             :body body)))
      
(defn request!
  "Perform a request"
  [url & opts]
  (let [parsed (parse-url url)
        opts' (apply hash-map opts)
        opts'' (conj parsed opts')
        {:keys [host port reactor]
         :or {reactor peloton.reactor/*reactor*}} opts'']
    (dofut [chan (connect! reactor host port)
           wrote-request? (write-request! reactor chan opts'')
           response (when wrote-request? (read-response! reactor chan))]
        (when chan
          (.close chan))
        response)))
        
