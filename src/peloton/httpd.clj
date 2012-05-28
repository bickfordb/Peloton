(ns peloton.httpd
  (:use peloton.util)
  (:use peloton.reactor)
  (:use clojure.tools.logging)
  (:import java.io.ByteArrayOutputStream)
  (:import java.util.Vector)
  (:import java.nio.ByteBuffer)
  (:import java.net.InetSocketAddress)
  (:import java.nio.charset.Charset)
  (:import java.nio.channels.ServerSocketChannel)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.spi.SelectorProvider))

(set! *warn-on-reflection* true)

(def show-errors-on-close? false)
(defonce header-pat #"^\s*(\S+)\s*[:]\s*(.+)\s*$")
(defonce request-line-pat #"^\s*(\S+)\s+(\S+)\s+(\S+)\s*$")
(defonce BUFFER-SIZE 128)
(declare on-404)

(defrecord Header 
  [^String name
   ^String value])

(defn get-header
  ^String
  [#^java.util.Vector headers 
   #^String header-name] 
  (let [n (count headers)]
    (loop [i 0]
      (when (< i n)
        (let [#^Header h (.get headers i)]
          (if (.equalsIgnoreCase header-name (.name h))
            (.value h)
            (recur (inc i))))))))

(definterface IRequest
  (^String method [])
  (setMethod [^String m])

  (^String uri [])
  (setURI [^String uri])

  (^java.util.Vector headers [])

  (^String protocol [])
  (setProtocol [^String s])

  (^bytes body [])
  (setBody [^bytes bs])
)

(deftype Request
  [^String ^{:volatile-mutable true} method
   ^String ^{:volatile-mutable true} uri
   ^String ^{:volatile-mutable true} protocol
   ^java.util.Vector headers
   ^bytes ^{:volatile-mutable true} body]
  IRequest
  (method [this] method)
  (setMethod [this v] (set! method v))
  (uri [this] uri)
  (setURI [this v] (set! uri v))
  (protocol [this] protocol)
  (setProtocol [this v] (set! protocol v))
  (headers [this] headers))

(defn empty-request
  []
  (peloton.httpd.Request. 
    "GET"
    "/"
    "HTTP/1.1"
    (java.util.Vector. )
    empty-bytes))

(definterface IResponse
  (^int status [])
  (setStatus [^int v])
  
  (^String message [])
  (setMessage [^String msg])

  (^java.util.Vector headers [])

  (^bytes body []) 
  (setBody [^bytes v]))

(deftype Response
  [^int ^{:volatile-mutable true} status
   ^String  ^{:volatile-mutable true} message
   ^java.util.Vector ^{:volatile-mutable true} headers
   ^bytes  ^{:volatile-mutable true} body]
  IResponse
  (setStatus [this s] (set! status s))
  (status [this] status)
  
  (setMessage [this m] (set! message m)) 
  (message [this] message)

  (headers [this] headers)
  
  (setBody [this b] (set! body b))
  (body [this] body))

(defn empty-response 
  []
  (peloton.httpd.Response.
    200
    "OK"
    (Vector. )
    nil))

(definterface IConnectionState
  (^peloton.util.LineParser lineParser [])
  (^peloton.httpd.Request request [])
  (^peloton.httpd.Response response [])
  (^java.nio.ByteBuffer inBuffer []) 
  (^java.util.Vector outBuffers []) 
  (^java.util.Vector inBytes []) 
  (^java.nio.channels.SocketChannel socketChannel [])
  (handler [])
   
  (^boolean finished)
  (setFinished [^boolean v])

  (state)
  (setState [st])

  (^boolean isSending)
  (setIsSending [^boolean st])
  )

(deftype ConnectionState
  [^java.nio.channels.SocketChannel socket-channel
   ^peloton.httpd.Request request
   ^peloton.httpd.Response response
   ^peloton.util.LineParser line-parser
   ^java.nio.ByteBuffer in-buffer
   ^java.util.Vector out-buffers
   ^boolean ^{:volatile-mutable true} finished 
   ^boolean ^{:volatile-mutable true} is-sending 
   ^{:volatile-mutable true} state
   handler]
  IConnectionState
  (request [this] request)
  (lineParser [this] line-parser)
  (response [this] response)
  (inBuffer [this] in-buffer)
  (outBuffers [this] out-buffers)
  (socketChannel [this] socket-channel)
  (handler [this] handler)

  (finished [this] finished)
  (setFinished [this v] (set! finished v))

  (state [this] state)
  (setState [this v] (set! state v))

  (isSending [this] is-sending)
  (setIsSending [this v] (set! is-sending v)))

(defn empty-connection-state 
  ^ConnectionState
  [socket-channel request-handler] 
  (peloton.httpd.ConnectionState.
    socket-channel
    (empty-request)
    (empty-response)
    (line-parser)
    (let [buf (ByteBuffer/allocate 1024)]
      (.flip buf)
      buf)
    (java.util.Vector.)
    false
    false
    0
    request-handler))

(def ^:dynamic #^ConnectionState *conn*)
(defn get-connection-state [] (.state *conn*))
(defn set-connection-state! [s] (.setState *conn* s))
(defn set-response-status! [st] (.setStatus (.response *conn*) st))
(defn set-response-message! [m] (.setMessage (.response *conn*) m))

(defn set-response-body!
  [^String s] 
  (.setBody (.response *conn*) (.getBytes s #^Charset UTF-8)))

(defn on-selection-key 
  [stored-connection-state f fargs] 
  (binding [*conn* stored-connection-state] (apply f fargs))
  (.cancel selection-key))

(defn on-socket-write-ready 
  [f & args]
  (.register (.socketChannel *conn*) selector SelectionKey/OP_WRITE [on-selection-key *conn* f args]))

(defn on-socket-read-ready 
  [f & args]
  (.register (.socketChannel *conn*) selector SelectionKey/OP_READ [on-selection-key *conn* f args]))

(defn fill-in-buffer! 
  "Try to fill the in buffer
  
  Returns
  the number of bytes read
  "
  []
  (let [b (.inBuffer *conn*)]
    (.compact b)
    (let [ret (.read (.socketChannel *conn*) b)]
      (.flip b)
      ret)))

(defn bind!
  [ports backlog]
  (let [ssc (ServerSocketChannel/open)]
    (.configureBlocking ssc false)
    (doseq [port ports] 
      (.bind (.socket ssc) (InetSocketAddress. port) backlog))
    ssc))

(defn add-response-header!
 [name value]
  (.add (.headers (.response *conn*)) (Header. name value)))

(defn close-conn!
  []
  (-> *conn* (.socketChannel) (.close)))

(defn close-with-error!
  [msg]
  (when show-errors-on-close?
    (let [out-buffer-s (apply list (for [[b f] (.outBuffers *conn*)]
                                     (dump-buffer b)))
          in-buffer-s (dump-buffer (.inBuffer *conn*))
          msg0 (format "closing connection on error: %s, in-buffer: %s, out-buffers: %s" 
                       msg in-buffer-s out-buffer-s)]
      (error msg0)))
    (close-conn!))

(defn socket-flush0!
  []
  (-> *conn* (.socketChannel) (.socket) (.getOutputStream) (.flush)))

(defn flush-output!
  []
  (-> *conn* (.outBuffers) (.add [(ByteBuffer/wrap empty-bytes) socket-flush0!])))

(defn send-buffers0!
  []
  (loop []
    (if (.isEmpty (.outBuffers *conn*))
      (let [] 
        (.setIsSending *conn* false)
        (when (.finished *conn*) 
          (close-conn!)))
      (let [[^ByteBuffer buffer f] (.get (.outBuffers *conn*) 0)]
        (if (> (.remaining buffer) 0)
          (condp = (safe -1 (.write (.socketChannel *conn*) buffer))
            -1 (close-with-error! "failed to write body")
            0 (on-socket-write-ready send-buffers0!)
            (recur))
          (let []
            (.remove (.outBuffers *conn*) 0)
            (when (not (nil? f)) (f))
            (recur)))))))

(defn send-buffers!
  []
  (when (not (.isSending *conn*))
    (.setIsSending *conn* true)
    (send-buffers0!)))

(defn write-bytes! 
  [^bytes b]
  (-> *conn* (.outBuffers) (.add [(ByteBuffer/wrap b) nil]))
  (send-buffers!))
  
(defn write-string!
  [^String s] 
  (write-bytes! (.getBytes s #^Charset UTF-8)))

(defn send-headers!
  []
  (let [xs (transient [])]
    (conj! xs "HTTP/1.1 ")
    (conj! xs (-> *conn* (.response) (.status) (str)))
    (conj! xs " ")
    (conj! xs (-> *conn* (.response) (.message)))
    (conj! xs "\r\n")
    (doseq [#^peloton.httpd.Header h (-> *conn* (.response) (.headers))]
      (conj! xs (.name h))
      (conj! xs ": ")
      (conj! xs (.value h))
      (conj! xs "\r\n"))
    (conj! xs "\r\n")
    (write-string! (apply str (persistent! xs)))))

(defn finish-response!
  []
  (.setFinished *conn* true)
  (send-buffers!))

(defn send-response!
  []
  (let [response (-> *conn* (.response))
        headers (-> response (.headers))
        body (-> response (.body))]
    (when (nil? (get-header headers "Content-Length")) 
      (-> headers (.add (Header. "Content-Length" (-> body (count) (str))))))
    (send-headers!)
    (when (not (nil? body))
      (write-bytes! body))
    (finish-response!)))

(defn start-chunked-response!
  []
  (let [response (-> *conn* (.response))
        headers (-> response (.headers))
        body (-> response (.body))]
    (when (not (= (get-header headers "Transfer-Encoding") "chunked"))
      (-> headers (.add (Header. "Transfer-Encoding" "chunked"))))
    (send-headers!)))

(defn send-chunk-bytes!
  [^bytes b]
  (write-string! (format "%X\r\n" (count b)))
  (write-bytes! b)
  (write-string! "\r\n"))

(defn send-chunk!
  [^String s]
  (send-chunk-bytes! (.getBytes s #^Charset UTF-8)))
  
(defn finish-chunked-response!
  []
  (write-string! "0\r\n\r\n")
  (finish-response!))

(defn drain-line!
  []
  (.put (.lineParser *conn*) (.inBuffer *conn*)))

(defn write-output-stream! 
  [^java.io.OutputStream os
   ^ByteBuffer buf 
   ^long amt]
  (let [offset (.position buf) 
        p (min (.remaining buf) (max amt 0))]
    (.write os (.array buf) offset p)
    (.position buf (+ offset p))))

(defn read-body0! 
  [^long len
   ^ByteArrayOutputStream bs]
  (loop []
    (let [remaining (max 0 (- len (.size bs)))
          in-buffer (.inBuffer *conn*)]
      (if (= remaining 0)
        ; run handler
        (let [h (.handler *conn*)]
          (.setState *conn* :processing)
          (if (nil? h) (on-404) (h)))
        (if (= 0 (.remaining in-buffer))
          (condp = (fill-in-buffer!)
            -1 (close-with-error! "failed to read body")
            0 (on-socket-read-ready read-body0! len bs)
            (recur))
          (let []
            (write-output-stream! bs in-buffer remaining)
            (recur)))))))

(defn read-body! 
  []
  (let [content-length (safe-int (get-header (.headers (.request *conn*)) "Content-Length"))]
    (let [in-content-length (max 0 (if (nil? content-length) 0 content-length))
          in-bytes (ByteArrayOutputStream. )]
      (read-body0! in-content-length in-bytes))))

(defn parse-header-line 
  [^String line]
  (let [m (re-find header-pat line)]
    (when m [(get m 1) (get m 2)])))

(defn read-header!
  []
  (loop [line (drain-line!)]
    (cond
      (nil? line) (condp = (fill-in-buffer!)
                    -1 (close-with-error! "cant read header")
                    0 (on-socket-read-ready read-header!)
                    (recur (drain-line!)))
      (= line "\r\n") (read-body!) ; "\r\n"
      :else (let [kv (parse-header-line line)
                  [k v] kv]
              (if (nil? kv) 
                (close-with-error! "invalid header") ; close on invalid header
                (let []
                  (.add (.headers (.request *conn*)) (Header. k v))
                  (recur (drain-line!))))))))

(defn parse-request-line
  [^String line] 
  (let [m (re-find request-line-pat line)]
    (when m
      {:method (.toUpperCase #^String (get m 1))
       :uri (get m 2)
       :protocol (get m 3)})))

(defn process-request-line!
  [^String request-line]
  (let [p (parse-request-line request-line)]
    (if (nil? p)
      (close-with-error! "couldn't parse request line") ; couldn't parse request line
      (let []
        (doto (.request *conn*)
          (.setURI (:uri p))
          (.setMethod (:method p))
          (.setProtocol (:protocol p)))
        (read-header!)))))

(defn read-request-line!
  []
  (loop []
    (let [line (drain-line!)]
      (if (nil? line)
        (condp = (fill-in-buffer!)
          -1 (close-with-error! "couldnt fill in-buffer for request line")
          0 (on-socket-read-ready read-request-line!)
          (recur))
        (process-request-line! line)))))

(defn on-accept 
  [request-handler] 
  (let [^ServerSocketChannel ssc (.channel selection-key)
        ^SocketChannel socket-channel (.accept ssc)]
    (when (not (nil? socket-channel))
      (let [socket (.socket socket-channel)]
        (.setTcpNoDelay socket false)
        (.setSoLinger socket false 0)
        (.setKeepAlive socket true)
        (.setReuseAddress socket true))
      (.configureBlocking socket-channel false)
      (binding [*conn* (empty-connection-state socket-channel request-handler)]
        (read-request-line!)))))

(defn on-404
  []
  (set-response-status! 404)  
  (set-response-message! "Not Found")  
  (add-response-header! "Content-Type" "text/html")
  (set-response-body! "<html><h1>404 Not Found</h1></html>")
  (send-response!))

(defn with-routes
  [routes]
  (fn []
    (loop [routes0 routes]
      (if (empty? routes0)
        (on-404)
        (let [[[pat-method pat-uri handler] & t] routes0
              request (.request *conn*)]
          (if (or (= pat-method :any) 
                  (= (name pat-method) (.method request)))
            (let [match (re-find pat-uri (.uri request))]
              (if match 
                (apply handler (rest match))
                (recur t)))
            (recur t)))))))

(defn listen!
  "Listen for connections"
  [^ServerSocketChannel server-socket-channel 
   handler]
  (.register server-socket-channel selector SelectionKey/OP_ACCEPT [on-accept handler]))

(defmacro later-with-conn!
  "Run this block seconds later with the same connection"
  [seconds & xs]
  `(let [conn# *conn*]
     (later ~seconds
            (binding [*conn* conn#] ~@xs))))

(defn serve!
  [opts 
   & routes]
  (let [{:keys [listen-backlog num-threads ports]} opts
        channel (bind! ports listen-backlog)
        f (fn []
            (with-reactor 
              (listen! channel (with-routes routes))
              (react)))
        threads (doseq [i (range num-threads)] (doto (Thread. f) (.start)))] 
    (doseq [^Thread t threads] (.join t))))

(defn write-file-channel-chunk!
  [^FileChannel channel 
   offset 
   len]

(defn write-file-channel!
  [^FileChannel channel 
   offset 
   len]
  (loop [offset offset
         len len]
    (let [amt (safe -1 (.transferTo offset len (.socketChannel *conn*)))]
      (cond
        (< amt -1) (close-with-error! "failed to write!")
        (= amt 0) (on-socket-write-ready write-file-channel! offset len)
        :else (recur (+ offset amt) (- len amt))))))

(defn create-file-handler
  [^String dir & opts]
  (let [opts0 (apply hash-map opts)]
    (fn [^String path] 
      (let [f (safe nil (java.io.RandomAccessFile. (File. (java.io.File. dir) path) "r"))
            ch (when (not-nil? f) (safe nil (.getChannel f)))
            offset 0
            content-type "application/octet-stream"
            sz (if (not-nil? f) (.size ch) 0)]
        (if (nil? ch)
          (on-404)
          (let []
            (add-response-header! "Content-Type" content-type)
            (add-response-header! "Content-Length" sz)
            (send-headers!)
            (write-file-channel! ch sz)))))))

