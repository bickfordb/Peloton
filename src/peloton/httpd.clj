(ns peloton.httpd
  (:use peloton.util)
  (:require peloton.mime)
  (:require [hiccup.core :as hiccup])
  (:require [peloton.io :as io])
  (:require peloton.reactor)
  (:use clojure.tools.logging)
  (:import clojure.lang.IFn)
  (:import clojure.lang.PersistentQueue)
  (:import java.io.ByteArrayOutputStream)
  (:import java.util.Vector)
  (:import java.nio.ByteBuffer)
  (:import java.net.InetSocketAddress)
  (:import java.nio.charset.Charset)
  (:import java.nio.channels.FileChannel)
  (:import java.io.File)
  (:import java.io.RandomAccessFile)
  (:import java.nio.channels.ServerSocketChannel)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.spi.SelectorProvider))

(set! *warn-on-reflection* true)

(defonce header-pat #"^\s*(\S+)\s*[:]\s*(.+)\s*$")
(defonce request-line-pat #"^\s*(\S+)\s+(\S+)\s+(\S+)\s*$")
(declare not-found!)

(defn get-header
  ^String [headers #^String header-name] 
  (loop [headers headers]
    (when (not (empty? headers))
      (let [[[^String k v] & t] headers]
        (if (.equalsIgnoreCase header-name k) 
          v 
          (recur t)))))) 

(defprotocol IBody 
  (^String protocol [r])
  (set-protocol! [r ^String s])
  (^PersistentQueue headers [r])
  (set-headers! [r ^PersistentQueue hs])
  (^bytes body [r])
  (set-body! [r ^bytes b]))

(defprotocol IRequest
  (^String method [r])
  (set-method! [r ^String s])
  (^String uri [r])
  (set-uri! [r ^String s]))

(deftype Request
  [^String ^:volatile-mutable method
   ^String ^:volatile-mutable uri
   ^String ^:volatile-mutable protocol
   ^PersistentQueue ^:volatile-mutable headers
   ^bytes ^:volatile-mutable body]
  IRequest
  (method [this] method)
  (set-method! [this s] (set! method s))
  (uri [this] uri)
  (set-uri! [this s] (set! uri s))
  IBody
  (protocol [this] protocol)
  (set-protocol! [this s] (set! protocol s))
  (headers [this] headers)
  (set-headers! [this hs] (set! headers hs))
  (body [this] body)
  (set-body! [this b] (set! body b)))

(defn empty-request 
  [] 
  (Request. 
    "GET" 
    "/"
    "HTTP/1.1"
    (PersistentQueue/EMPTY)
    empty-bytes))

(defprotocol IResponse
  (status [r])
  (set-status! [r s])
  (^String message [r])
  (set-message! [r ^String message]))

(deftype Response
  [^:volatile-mutable status
   ^String ^:volatile-mutable message
   ^String ^:volatile-mutable protocol
   ^PersistentQueue ^:volatile-mutable headers
   ^bytes ^:volatile-mutable body]
  IResponse
  (status [this] status)
  (set-status! [this s] (set! status s))
  (message [this] message)
  (set-message! [this m] (set! message m))
  IBody
  (protocol [this] protocol)
  (set-protocol! [this s] (set! protocol s))
  (headers [this] headers)
  (set-headers! [this hs] (set! headers hs))
  (body [this] body)
  (set-body! [this b] (set! body b)))

(def date-fmt (doto 
                (java.text.SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss z")
                (.setTimeZone (java.util.TimeZone/getTimeZone "GMT"))))


(defn empty-response 
  []
  (Response.
    200
    "OK"
    "HTTP/1.1"
    (conj PersistentQueue/EMPTY
     ["Server" "peloton/-inf"]
     ["Date" (.format #^java.text.SimpleDateFormat date-fmt (java.util.Date.))])
    empty-bytes))
 
(defprotocol IConnection
 (^peloton.reactor.Reactor reactor [conn])
 (^SocketChannel socket-channel [conn])
 (^Request request [conn])
 (^Response response [conn])
 (set-out-buffers! [conn ^PersistentQueue queue])
 (^PersistentQueue out-buffers [conn])
 (^ByteBuffer in-buffer [conn])
 (finish! [conn])
 (^boolean finished? [conn])
 (^boolean sending? [conn])
 (set-sending! [conn sending?])
 (request-handler [conn]))

(deftype Connection
  [^peloton.reactor.Reactor reactor
   ^SocketChannel socket-channel
   ^Request request
   ^Response response
   ^PersistentQueue ^:volatile-mutable out-buffers
   ^ByteBuffer in-buffer
   ^boolean ^:volatile-mutable finished?
   ^boolean ^:volatile-mutable sending?
   ^IFn request-handler]
  IConnection
  (reactor [this] reactor)
  (socket-channel [this] socket-channel)
  (request [this] request)
  (response [this] response)
  (out-buffers [this] out-buffers)
  (set-out-buffers! [this bs] (set! out-buffers bs))
  (in-buffer [this] in-buffer)
  (finished? [this] finished?)
  (finish! [this] (set! finished? (boolean true)))
  (sending? [this] sending?)
  (set-sending! [this x?] (set! sending? (boolean x?)))
  (request-handler [this] request-handler))
 
(defn empty-connection 
  ^Connection
  [reactor socket-channel request-handler in-buffer-size] 
  (Connection.
    reactor 
    socket-channel
    (empty-request)
    (empty-response)
    (PersistentQueue/EMPTY)
    (let [b (java.nio.ByteBuffer/allocate in-buffer-size)]
      (.flip b)
      b)
    false
    false
    request-handler))

(defn set-response-status! 
  [^Connection conn ^long st] 
  (set-status! (response conn) st))

(defn set-response-message! 
  [^Connection conn ^String m] 
  (set-message! (response conn) m)) 

(def uri-pat #"^([^?#]+)(?:[?]([^#]+))?(?:[#](.+))?$")

(defn parse-uri
  [uri]
  (cond 
    (nil? uri) {:path "" 
                :query ""
                :fragment ""}
    :else (let [match (re-find uri-pat uri)
                [_ path qs fragment] match]
            {:path (if path path "")
             :query (if qs qs "")
             :fragment (if fragment fragment "")})))

(defn query-data
  "Load the URL encoded data from the query string in the request line URI"
  [^Connection conn]
  (let [{:keys [query]} (parse-uri (uri (request conn)))]
   (parse-qs query)))

(defn post-data
  "Decode the \"URL-encoded\" (usually POST) data from the request body"
  [^Connection conn]
  ; FIXME: read the character set in from the request headers
  ; FIXME: handle multi-part requests
  (let [^bytes body (body (request conn))
        s (safe "" (String. body #^Charset UTF-8))
        data (parse-qs s)]
    data))

(defn post-data-map
  [conn]
  (apply hash-map (apply concat (post-data conn))))

(defn set-response-body!
  "Set the response body"
  [^Connection conn ; the connection
   ^String s ; the body to be encoded as UTF-8
   ]
  (set-body! (response conn) (.getBytes s #^Charset UTF-8)))

(defn bind!
  [ports backlog]
  (let [ssc (ServerSocketChannel/open)]
    (.configureBlocking ssc false)
    (doseq [port ports] 
      (.bind (.socket ssc) (InetSocketAddress. port) backlog))
    ssc))

(defn add-response-header!
 [^Connection conn name value]
  (set-headers! 
    (response conn) 
    (conj (headers (response conn)) [name value])))

(defn set-response-header! 
  "Set a response header"
  [^Connection conn 
   ^String header-name 
   ^String header-value]
  (set-headers! (response conn)
        (conj
          (filter (fn [[^String h v]] (not (.equalsIgnoreCase h header-name))) (headers (response conn)))
          [header-name header-value])))

(defn set-content-type! 
  [conn content-type] 
  (set-response-header! conn "Content-Type" content-type))

(defn set-content-type-html!
  [conn] 
  (set-response-header! conn "Content-Type" "text/html; charset=utf8"))

(defn set-content-type-json!
  [conn] 
  (set-response-header! conn "Content-Type" "application/json"))

(defn close-conn!
  [^Connection conn]
  (finish! conn)
  (let [bs (out-buffers conn)]
    (set-out-buffers! conn PersistentQueue/EMPTY)
    (doseq [[b f] bs]
      (when f
        (f false))))
  (.close #^SocketChannel (.socket-channel conn)))

(defn socket-flush0!
  [^Connection conn]
  (.flush (.getOutputStream (.socket #^SocketChannel (socket-channel conn)))))

(defn flush-output!
  [^Connection conn]
  (set-out-buffers! conn (conj (out-buffers conn) [nil (fn [succ?] (socket-flush0! conn))])))

;(defn send-buffers!
;  [^Connection conn]
;  (loop []
;    (cond 
;      (empty? (out-buffers conn)) (do 
;                                      (set-sending! conn false)
;                                      (when (finished? conn)
;                                        (close-conn! conn)))
;      :else (let [[^ByteBuffer buffer f] (first (out-buffers conn))]
;              (cond 
;                (and buffer (> (.remaining buffer) 0)) (condp 
;                                            = 
;                                            (safe -1 (.write #^SocketChannel (socket-channel conn) buffer))
;                                            -1 (close-conn! conn) 
;                                            0 (peloton.reactor/on-writable-once! (socket-channel conn) send-buffers! conn)
;                                            (recur))
;                :else (do 
;                        (set-out-buffers! conn (rest (out-buffers conn)))
;                        (when f (f true))
;                        (recur)))))))

(def barr (into-array ByteBuffer []))

(defn send-buffers!'
  [^Connection conn]
  (loop []
    (cond 
      (empty? (out-buffers conn)) (do 
                                      (set-sending! conn false)
                                      (when (finished? conn)
                                        (close-conn! conn)))
      :else (let [bufs (out-buffers conn)
                  [[^ByteBuffer buf f] & _] bufs]
              (cond 
                (or (not buf) (== (.remaining buf) 0)) (do 
                                                         (when f (f true))
                                                         (set-out-buffers! conn (rest (out-buffers conn)))
                                                         (recur))
                :else (let [non-empty-bufs (remove #(and % (== (.remaining #^ByteBuffer %) 0)) (map first bufs))
                            ^"[Ljava.nio.ByteBuffer;" buf-arr (.toArray #^java.util.Collection non-empty-bufs #^"[Ljava.nio.ByteBuffer;" barr) 
                            ;^"[Ljava.nio.ByteBuffer;" buf-arr (into-array 
                            ;                                    java.nio.ByteBuffer
                            ;                                    non-empty-bufs)
                            ;amt (safe -1 (.write (socket-channel conn) buf-arr))]
                            amt (.write (socket-channel conn) buf-arr)]
                        ;(println "write " amt)
                        (condp = amt 
                          -1 (close-conn! conn) 
                          0 (peloton.reactor/on-reactor-writable-once! 
                              (reactor conn)
                              (socket-channel conn)
                              send-buffers!' conn)
                          (recur))))))))
                          
(defn write-buffer!
  "Write a buffer"
  [^Connection conn
   ^ByteBuffer buffer
   f]
  ;(when (finished? conn)
  ;  (throw (Exception. "write to a closed connection")))
  (set-out-buffers! conn (conj (out-buffers conn) [buffer f]))
  (when (not (sending? conn))
    (set-sending! conn true)
    (send-buffers!' conn)))

(defn write-bytes! 
  [^Connection conn
   ^bytes b 
   ^IFn f] ; callable
  (write-buffer! conn (ByteBuffer/wrap b) f))
  
(defn write-string!
  [^Connection conn
   ^String s
   ^IFn f ; callable
   ] 
  (write-bytes! conn (.getBytes s #^Charset UTF-8) f))

(defn send-headers!
  [^Connection conn
   ^IFn after]
  (let [xs (StringBuffer.)
        ^Response response (response conn)]
    (.append xs "HTTP/1.1 ")
    (.append xs #^long (status response))
    (.append xs " ")
    (.append xs (message response))
    (.append xs "\r\n")
    (doseq [[name value] (headers response)]
      (.append xs name)
      (.append xs ": ")
      (.append xs value)
      (.append xs "\r\n"))
    (.append xs "\r\n")
    (write-string! conn (.toString xs) after)))

(defn finish-response!
  [^Connection conn]
  (finish! conn)
  (when (empty? (out-buffers conn))
    (close-conn! conn)))

(defn send-response!
  [^Connection conn]
  (let [^Response response (response conn)
        headers (headers response)
        body (body response)]
    (when (nil? (get-header headers "Content-Length")) 
      (add-response-header! conn "Content-Length" (str (count body))))
    (send-headers! conn nil)
    (when body
      (write-bytes! conn body nib))
    (finish-response! conn)))

(defn start-chunked-response!
  [^Connection conn]
  (let [^Response response (response conn)
        headers (headers response)
        body (body response)]
    (when (not (= (get-header headers "Transfer-Encoding") "chunked"))
      (add-response-header! conn "Transfer-Encoding" "chunked"))
    (send-headers! conn nil)))

(defn send-chunk-bytes!
  [^Connection conn
   ^bytes b]
  (write-string! conn (format "%X\r\n" (count b)) nil)
  (write-bytes! conn b nil)
  (write-string! conn "\r\n" nil))

(defn send-chunk!
  [^Connection conn
   ^String s]
  (send-chunk-bytes! conn (.getBytes s #^Charset UTF-8)))
  
(defn finish-chunked-response!
  [^Connection conn]
  (write-string! conn "0\r\n\r\n" nil)
  (finish-response! conn))

(defn fill-in-buffer! 
  [conn after]
  (let [buf (in-buffer conn)
        chan (socket-channel conn)]
    (.compact buf)
    (let [n (.read chan buf)]
      (.flip buf)
      (condp = n
        -1 (close-conn! conn)
        0 (peloton.reactor/on-reactor-readable-once! (reactor conn) chan fill-in-buffer! conn after)
        (after)))))

(defn on-body
  [^Connection conn
   ^ByteBuffer buffer]
  (cond 
    (nil? buffer) (close-conn! conn)
    :else (do 
            (set-body! (request conn) (.array buffer))
            ((request-handler conn) conn))))

(defn request-content-length
  "Get the content length of the request"
  [^Connection conn]
  (safe-int (get-header (headers (request conn)) "Content-Length")))

(defn parse-header-line 
  [^String line]
  (let [m (re-find header-pat line)]
    (when m [(get m 1) (get m 2)])))

(defn parse-request-line
  [^String line] 
  (let [m (re-find request-line-pat line)]
    (when m
      {:method (.toUpperCase #^String (get m 1))
       :uri (get m 2)
       :protocol (get m 3)})))


(defn read-to-buf! 
  [conn ^ByteBuffer buffer f]
  (let [in-buffer (in-buffer conn)
        chan (socket-channel conn)]
    (loop []
      (cond 
        (= (.remaining buffer) 0) (f)
        (> (.remaining in-buffer) 0) (do (.put buffer in-buffer)
                                         (recur))
        :else (do
                (.compact in-buffer)
                (let [n (.read chan in-buffer)]
                  (.flip in-buffer)
                  (condp = n
                    -1 (f)
                    0 (peloton.reactor/on-reactor-readable-once! (reactor conn) chan read-to-buf! conn buffer f)
                    (recur))))))))

(defn read-body!
  [conn len]
  (let [a-buf (ByteBuffer/allocate len)]
    (read-to-buf! 
      (socket-channel conn)
      a-buf
      #(if (= (.remaining a-buf) len)
         (do (set-body! (request conn) (.array a-buf))
           ((request-handler conn) conn))
         (close-conn! conn)))))

(defn check-body
  [conn]
  (let [content-length (request-content-length conn)
        content-length' (max content-length 0)]
    (if (> content-length' 0)
      (read-body! conn content-length'))))
      

(defn process-headers!
  [conn ^bytes some-bytes]
  (let [a-string (String. some-bytes #^Charset UTF-8)
        all-header-lines (seq (.split a-string "\r\n"))
        [request-line & other-lines] all-header-lines
        request-parsed (parse-request-line request-line)
        {:keys [uri method protocol]} request-parsed
        headers (map parse-header-line (rest all-header-lines))
        headers' (remove nil? headers)
        request (request conn)]
        (when uri 
          (set-uri! request uri))
        (when method
          (set-method! request method))
        (when protocol 
          (set-protocol! request protocol))
        (set-headers! request headers')
        ((request-handler conn) conn)))

(defn read-headers'! 
  [conn ^ByteArrayOutputStream bs line-len]
  (let [buf (in-buffer conn)]
     (loop [line-len line-len]
      (cond
        (= (.remaining buf) 0) (fill-in-buffer! conn #(read-headers'! conn bs line-len))
        :else (do
                (let [a-byte (.get buf)]
                  (.write bs (int a-byte))
                  (cond 
                    ; if we get a newline and the line length is 1 (just a /cr) we're done
                    (and (== a-byte 10) (= line-len 1)) (process-headers! conn (.toByteArray bs))
                    (== a-byte 10) (recur 0)
                    ; FIXME: check for excessively long headers here?
                    :else (recur (inc line-len)))))))))

(defn read-headers! 
  [conn] 
  (let [bs (ByteArrayOutputStream. 128)]
    (read-headers'! conn bs 0)))

(defn on-accept 
  [reactor request-handler] 
  (let [^ServerSocketChannel server-socket-chan (.channel peloton.reactor/selection-key)]
    (loop [^SocketChannel socket-channel (.accept server-socket-chan)]
      (when socket-channel
        (let [socket (.socket socket-channel)
              conn (empty-connection reactor socket-channel request-handler 1024)]
          (.configureBlocking socket-channel false)
          (.setTcpNoDelay socket false)
          (.setSoLinger socket false 0)
          (.setKeepAlive socket true)
          (.setReuseAddress socket true)
          (read-headers! conn)
          (recur (.accept server-socket-chan)))))))


(defmacro send-html!
  [conn & body]
  `(let [conn# ~conn]
     (set-content-type-html! conn#)
     (set-response-body! conn# (hiccup/html ~@body))
     (send-response! conn#)))

(defn not-found!
  [^Connection conn]
  (set-response-status! conn 404)  
  (set-response-message! conn "Not Found")  
  (send-html! conn [:body [:h1 "404 Not Found"]]))

(defn see-other! 
  [conn uri]
  (set-response-header! conn "Location" uri)
  (set-response-status! conn 303)
  (set-response-message! conn "See Other")
  (send-html! conn [:body [:h1 "303 See Other"]]))

(defn with-routes
  [routes]
  #(let [request (request #^Connection %)
          uri-info (parse-uri (uri request))
          {:keys [path]} uri-info]
      (loop [routes0 routes]
        (if (empty? routes0)
          (not-found! %)
          (let [[[pat-method pat-uri handler] & t] routes0]
            (if (or (= pat-method :any) 
                    (= (name pat-method) (method request)))
              (let [[match & match-args] (re-find0 pat-uri path)]
                (if match 
                  (apply handler % match-args)
                  (recur t)))
              (recur t)))))))

(defn listen!
  "Listen for connections"
  [^ServerSocketChannel server-socket-channel 
   handler]
  (peloton.reactor/on-acceptable! server-socket-channel on-accept peloton.reactor/*reactor* handler))

(defn serve!
  [opts 
   & routes]
  (let [{:keys [listen-backlog ports] 
         :or {listen-backlog 100 ports [8080]}} opts
        channel (bind! ports listen-backlog)]
      (spread 
        (peloton.reactor/with-reactor 
          (listen! channel (with-routes routes))))))

(defn write-file-channel!
  [^Connection conn
   ^FileChannel channel 
   offset 
   len]
  (loop [offset (max offset 0)
         len (max len 0)]
    (if (> len 0)
      (let [amt (safe -1 (.transferTo channel offset len (socket-channel conn)))]
        (cond
          (< amt 0) (close-conn! conn)
          (= amt 0) (peloton.reactor/on-reactor-writable-once! (reactor conn) (socket-channel conn) write-file-channel! conn channel offset len)
          :else (recur (+ offset amt) (- len amt))))
      (finish-response!))))

(def range-pat #"bytes=(\d+)(?:-(\d+))?")

(defn parse-range-header
  [s]
  (let [m (when s (re-find range-pat s))
        [a b c] m]
    {:range-start-byte (safe-int b)
     :range-end-byte (safe-int c)}))

(defn create-file-handler
  [^String dir & opts]
  (let [opts0 (apply hash-map opts)
        mime-types (get opts0 :mime-types peloton.mime/common-ext-to-mime-types)]
    (fn [^Connection conn 
         ^String path] 
      (let [^RandomAccessFile f (safe nil (java.io.RandomAccessFile. (File. (java.io.File. dir) path) "r"))
            ch (when (not-nil? f) (safe nil (.getChannel f)))]
        (if (nil? ch)
          (not-found! conn)
          (let [sz (safe 0 (.length f))
                len sz
                content-type "application/octet-stream"
                range-header (get-header (headers (request conn)) "Range")
                {:keys [range-start-byte range-end-byte]} (parse-range-header range-header)
                offset (if (nil? range-start-byte) 0 range-start-byte)
                len (if (nil? range-end-byte) 
                      (- sz offset) 
                      (+ (inc (- range-end-byte range-start-byte))))
                after-headers (fn [] (write-file-channel! conn ch offset len))]
            (add-response-header! conn "Content-Type" (peloton.mime/guess-mime-type-of-path path mime-types))
            (add-response-header! conn "Accept-Ranges" "bytes")
            (add-response-header! conn "Content-Length" len)
            (when (or (> offset 0)
                      (not (= len sz)))
              (set-response-status! conn 206)
              (set-response-message! conn "Partial Content")
              (let [start-byte offset
                    end-byte (dec (+ offset len))]
                (add-response-header! conn "Content-Range" (format "%d-%d/%d" start-byte end-byte sz))))
            (send-headers! conn nil)
            (write-buffer! conn (ByteBuffer/wrap empty-bytes) after-headers)))))))

