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

(defonce show-errors-on-close false)
(defonce header-pat #"^\s*(\S+)\s*[:]\s*(.+)\s*$")
(defonce request-line-pat #"^\s*(\S+)\s+(\S+)\s+(\S+)\s*$")
(defonce BUFFER-SIZE 128)

(defrecord Header 
  [^String name
   ^String value])

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
    empty-bytes))

(definterface IConnectionState
  (^peloton.util.LineParser lineParser [])
  (^peloton.httpd.Request request [])
  (^peloton.httpd.Response response [])
  (^java.nio.ByteBuffer inBuffer []) 
  (^java.util.Vector outBuffers []) 
  (^java.util.Vector inBytes []) 
  (^java.nio.channels.SocketChannel socketChannel [])
   
  (^boolean finished)
  (setFinished [^boolean v])

  (^int state)
  (setState [^int st])
  
  )

(deftype ConnectionState
  [^java.nio.channels.SocketChannel socket-channel
   ^peloton.httpd.Request request
   ^peloton.httpd.Response response
   ^peloton.util.LineParser line-parser
   ^java.nio.ByteBuffer in-buffer
   ^java.util.Vector out-buffers
   ^boolean ^{:volatile-mutable true} finished 
   ^int ^{:volatile-mutable true} state]
  IConnectionState
  (request [this] request)
  (lineParser [this] line-parser)
  (response [this] response)
  (inBuffer [this] in-buffer)
  (outBuffers [this] out-buffers)
  (socketChannel [this] socket-channel)

  (finished [this] finished)
  (setFinished [this v] (set! finished v))

  (state [this] state)
  (setState [this v] (set! state v)))

(defn empty-connection-state 
  ^ConnectionState
  [socket-channel] 
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
    0))

(def ^:dynamic #^ConnectionState *conn*)

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

(defn fill-in-buffer 
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

(defn bind 
  [ports backlog]
  (let [ssc (ServerSocketChannel/open)]
    (.configureBlocking ssc false)
    (doseq [port ports] 
      (.bind (.socket ssc) (InetSocketAddress. port) backlog))
    ssc))

(defn add-response-header!
 [name value]
  (.add (.headers (.response *conn*)) (Header. name value)))

(defn close-conn
  []
  (.close (.socket (.socketChannel *conn*))))

(defn close-error
  [msg]
  (when show-errors-on-close
    (let [out-buffer-s (apply list (for [b (.outBuffers *conn*)]
                                     (dump-buffer b)))
          in-buffer-s (dump-buffer (.inBuffer *conn*))
          msg0 (format "closing connection on error: %s, in-buffer: %s, out-buffers: %s" 
                       msg in-buffer-s out-buffer-s)]
      (error msg0)))
    (close-conn))

(defn safe-write
  [^SocketChannel socket-channel 
   ^ByteBuffer b]
  (try
    (.write socket-channel b)
    (catch Exception e -1)))

(defn send-buffers
  []
  (loop []
    (if (.isEmpty (.outBuffers *conn*))
      (when (.finished *conn*) (close-conn))
      (let [^ByteBuffer buffer (.get (.outBuffers *conn*) 0)]
        (if (> (.remaining buffer) 0)
          (let [amt (safe-write (.socketChannel *conn*) buffer)]
            (condp = amt
              -1 (close-error "failed to write body")
              0 (on-socket-write-ready send-buffers)
              (recur)))
          (let []
            (.remove (.outBuffers *conn*) 0)
            (recur)))))))

(defn write-string
  [^String s] 
  (let [buf (ByteBuffer/wrap (.getBytes s #^Charset ISO-8859-1))]
    (.add (.outBuffers *conn*) buf)))

(defn write-strings
  [xs]
  (let [bs0 (map #(.getBytes #^String % #^Charset ISO-8859-1) xs)
        n (reduce + (map #(count #^bytes %) bs0))
        buf (ByteBuffer/allocate n)]
    (doseq [#^bytes b bs0]
      (.put buf b))
    (.flip buf)
    (.add (.outBuffers *conn*) buf)))

(defn send-response
  []
  (let [xs (transient [])]
    (conj! xs "HTTP/1.1 ")
    (conj! xs (str (.status (.response *conn*))))
    (conj! xs " ")
    (conj! xs (.message (.response *conn*)))
    (conj! xs "\r\n")
    (doseq [#^peloton.httpd.Header h (.headers (.response *conn*))]
      (conj! xs (.name h))
      (conj! xs ": ")
      (conj! xs (.value h))
      (conj! xs "\r\n"))
    (conj! xs "\r\n")
    (write-string (apply str (persistent! xs)))))

(defn finish-response
  []
  (.setFinished *conn* true)
  (send-buffers))

(defn drain-line
  []
  (.put (.lineParser *conn*) (.inBuffer *conn*)))

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

(defn dispatch-request
  []
  (.setStatus (.response *conn*) 200)
  (when (> (.remaining (.inBuffer *conn*)) 0)
    (warn (.remaining (.inBuffer *conn*)) "unexpected request body bytes"))
  (add-response-header! "Content-Type" "text/html")
  (add-response-header! "Content-Length" "0")
  (send-response)
  (finish-response))

(defn write-output-stream 
  [^java.io.OutputStream os
   ^ByteBuffer buf 
   ^long amt]
  (let [offset (.position buf) 
        p (min (.remaining buf) (max amt 0))]
    (.write os (.array buf) offset p)
    (.position buf (+ offset p))))

(defn read-body0 
  [^long len
   ^ByteArrayOutputStream bs]
  (loop []
    (let [remaining (max 0 (- len (.size bs)))
          in-buffer (.inBuffer *conn*)]
      (if (= remaining 0)
        (dispatch-request)
        (if (= 0 (.remaining in-buffer))
          (condp = (fill-in-buffer)
            -1 (close-error "failed to read body")
            0 (on-socket-read-ready read-body0 len bs)
            (recur))
          (let []
            (write-output-stream bs in-buffer remaining)
            (recur)))))))

(defn read-body 
  []
  (let [content-length (safe-int (get-header (.headers (.request *conn*)) "Content-Length"))]
    (let [in-content-length (max 0 (if (nil? content-length) 0 content-length))
          in-bytes (ByteArrayOutputStream. )]
      (read-body0 in-content-length in-bytes))))

(defn parse-header-line 
  [^String line]
  (let [m (re-find header-pat line)]
    (when m [(get m 1) (get m 2)])))

(defn read-header
  []
  (loop [line (drain-line)]
    (cond
      (nil? line) (condp = (fill-in-buffer)
                    -1 (close-error "cant read header")
                    0 (on-socket-read-ready read-header)
                    (recur (drain-line)))
      (= line "\r\n") (read-body) ; "\r\n"
      :else (let [kv (parse-header-line line)
                  [k v] kv]
              (if (nil? kv) 
                (close-error "invalid header") ; close on invalid header
                (let []
                  (.add (.headers (.request *conn*)) (Header. k v))
                  (recur (drain-line))))))))

(defn parse-request-line
  [^String line] 
  (let [m (re-find request-line-pat line)]
    (when m
      {:method (get m 1)
       :uri (get m 2)
       :protocol (get m 3)})))

(defn process-request-line
  [^String request-line]
  (let [p (parse-request-line request-line)]
    (if (nil? p)
      (close-error "couldn't parse request line") ; couldn't parse request line
      (let []
        (doto (.request *conn*)
          (.setURI (:uri p))
          (.setMethod (:method p))
          (.setURI (:protocol p)))
        (read-header)))))

(defn read-request-line
  []
  (loop []
    (let [line (drain-line)]
      (if (nil? line)
        (condp = (fill-in-buffer)
          -1 (close-error "couldnt fill in-buffer for request line")
          0 (on-socket-read-ready read-request-line)
          (recur))
        (process-request-line line)))))

(defn on-accept 
  [] 
  (let [^ServerSocketChannel ssc (.channel selection-key)
        ^SocketChannel socket-channel (.accept ssc)]
    (when (not (nil? socket-channel))
      (let [socket (.socket socket-channel)]
        (.setTcpNoDelay socket false)
        (.setSoLinger socket false 0)
        (.setKeepAlive socket true)
        (.setReuseAddress socket true))
      (.configureBlocking socket-channel false)
      (binding [*conn* (empty-connection-state socket-channel)]
        (read-request-line)))))

(defn listen 
  [^ServerSocketChannel server-socket-channel]
  (.register server-socket-channel selector SelectionKey/OP_ACCEPT [on-accept]))


