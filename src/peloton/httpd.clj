(ns peloton.httpd
  (:use peloton.util)
  (:use peloton.reactor)
  (:require peloton.mime)
  (:require [hiccup.core :as hiccup])
  (:require [peloton.io :as io])
  (:require [peloton.reactor :as reactor])
  (:use clojure.tools.logging)
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

(defrecord Request
  [method
   uri
   protocol
   headers
   body])

(defn empty-request 
  [] 
  (Request. 
    (atom "GET")
    (atom "/")
    (atom "HTTP/1.1")
    (atom [])
    (atom empty-bytes)))

(defrecord Response
  [status
   message
   headers
   body])

(def date-fmt (doto 
                (java.text.SimpleDateFormat. "EEE, dd MMM yyyy HH:mm:ss z")
                (.setTimeZone (java.util.TimeZone/getTimeZone "GMT"))))

(defn empty-response 
  []
  (Response.
    (atom 200)
    (atom "OK")
    (atom [["Server" "peloton/-inf"]
           ["Date" (.format #^java.text.SimpleDateFormat date-fmt (java.util.Date.))]])
    (atom empty-bytes)))
  
(defrecord Connection
  [^SocketChannel socket-channel
   ^Request request
   ^Response response
   out-buffers
   finished?
   sending?
   request-handler])
 
(defn empty-connection 
  ^Connection
  [socket-channel request-handler] 
  (Connection.
    socket-channel
    (empty-request)
    (empty-response)
    (atom []) 
    (atom false)
    (atom false)
    request-handler))

(defn set-response-status! [^Connection conn ^long st] (reset! (.status #^Response (.response conn)) st))
(defn set-response-message! [^Connection conn ^String m] (reset! (.message #^Response (.response conn)) m)) 

(defn finished? 
  "Returns true if the connection is finished"
  [^Connection conn]
  @(.finished? conn))

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
  (let [{:keys [query]} (parse-uri @(.uri #^Request (.request conn)))]
   (parse-qs query)))

(defn post-data
  "Decode the \"URL-encoded\" (usually POST) data from the request body"
  [^Connection conn]
  ; FIXME: read the character set in from the request headers
  ; FIXME: handle multi-part requests
  (let [^bytes body @(.body #^Request (.request conn))
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
  (reset! (.body #^Response (.response conn)) (.getBytes s #^Charset UTF-8)))



(defn bind!
  [ports backlog]
  (let [ssc (ServerSocketChannel/open)]
    (.configureBlocking ssc false)
    (doseq [port ports] 
      (.bind (.socket ssc) (InetSocketAddress. port) backlog))
    ssc))

(defn add-response-header!
 [^Connection conn ^String name ^String value]
  (swap! (.headers #^Response (.response conn)) concat [[name value]]))

(defn set-response-header! 
  "Set a response header"
  [^Connection conn 
   ^String header-name 
   ^String header-value]
  (swap! (.headers #^Response (.response conn))
         (fn [hs] (concat
                    (filter (fn [[^String h v]] (not (.equalsIgnoreCase h header-name))) hs)
                    [[header-name header-value]]))))

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
  (reset! (.finished? conn) true)
  (let [bs @(.out-buffers conn)]
    (reset! (.out-buffers conn) [])
    (doseq [[b f] bs]
      (when f
        (f false))))
  (.close #^SocketChannel (.socket-channel conn)))

(defn socket-flush0!
  [^Connection conn]
  (.flush (.getOutputStream (.socket #^SocketChannel (.socket-channel conn)))))

(defn flush-output!
  [^Connection conn]
  (swap! (.out-buffers conn) conj [nil (fn [succ?] (socket-flush0! conn))]))

(defn send-buffers!
  [^Connection conn]
  (loop []
    (cond 
      (empty? @(.out-buffers conn)) (do 
                                      (reset! (.sending? conn) false)
                                      (when @(.finished? conn)
                                        (close-conn! conn)))
      :else (let [[[^ByteBuffer buffer f] & t] @(.out-buffers conn)]
              (cond 
                (and buffer (> (.remaining buffer) 0)) (condp 
                                            = 
                                            (safe -1 (.write #^SocketChannel (.socket-channel conn) buffer))
                                            -1 (close-conn! conn) 
                                            0 (reactor/on-writable-once! (.socket-channel conn) send-buffers! conn)
                                            (recur))
                :else (do 
                        (swap! (.out-buffers conn) rest)
                        (when f (f true))
                        (recur)))))))

(defn write-buffer!
  "Write a buffer"
  [^Connection conn
   ^ByteBuffer buffer
   f]
  (when @(.finished? conn)
    (throw (Exception. "write to a closed connection")))
  (swap! (.out-buffers conn) concat [[buffer f]])
  (when (not @(.sending? conn))
    (reset! (.sending? conn) true)
    (send-buffers! conn)))

(defn write-bytes! 
  [^Connection conn
   ^bytes b 
   f ; callable
   ]
  (write-buffer! conn (ByteBuffer/wrap b) f))
  
(defn write-string!
  [^Connection conn
   ^String s
   f ; callable
   ] 
  (write-bytes! conn (.getBytes s #^Charset UTF-8) f))

(defn send-headers!
  [^Connection conn
   after]
  (let [xs (StringBuffer.)
        ^Response response (.response conn)]
    (.append xs "HTTP/1.1 ")
    (.append xs #^long @(.status response))
    (.append xs " ")
    (.append xs @(.message response))
    (.append xs "\r\n")
    (doseq [[name value] @(.headers response)]
      (.append xs name)
      (.append xs ": ")
      (.append xs value)
      (.append xs "\r\n"))
    (.append xs "\r\n")
    (write-string! conn (.toString xs) after)))

(defn finish-response!
  [^Connection conn]
  (reset! (.finished? conn) true)
  (when (empty? @(.out-buffers conn))
    (close-conn! conn)))

(defn send-response!
  [^Connection conn]
  (let [^Response response (-> conn (.response))
        headers (.headers response)
        body @(-> response (.body))]
    (when (nil? (get-header @headers "Content-Length")) 
      (swap! headers conj ["Content-Length" (str (count body))]))
    (send-headers! conn nil)
    (when (not (nil? body))
      (write-bytes! conn body nib))
    (finish-response! conn)))

(defn start-chunked-response!
  [^Connection conn]
  (let [^Response response (.response conn)
        headers (.headers response)
        body (.body response)]
    (when (not (= (get-header @headers "Transfer-Encoding") "chunked"))
      (swap! headers conj ["Transfer-Encoding" "chunked"]))
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

(defn on-body
  [^Connection conn
   ^ByteBuffer buffer]
  (cond 
    (nil? buffer) (close-conn! conn)
    :else (do 
            (reset! (.body #^Request (.request conn)) (.array buffer))
            ((.request-handler conn) conn))))

(defn request-content-length
  "Get the content length of the request"
  [^Connection conn]
  (safe-int (get-header @(.headers #^Request (.request conn)) "Content-Length")))

(defn read-body! 
  [^Connection conn]
  (let [content-length (request-content-length conn)
        content-length0 (if (nil? content-length) 0 content-length)]
    (io/read-to-buf! (.socket-channel conn) content-length0 on-body conn)))

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

(defn on-header-line
  [^Connection conn
   ^String header-line]
  (cond
    (nil? header-line) (close-conn! conn)
    (= "\r\n" header-line) (read-body! conn)
    :else (let [h (parse-header-line header-line)]
            (cond
              (nil? h) (close-conn! conn)
              :else (do 
                      (swap! (.headers #^Request (.request conn)) concat [h])
                      (io/read-line! (.socket-channel conn) on-header-line conn))))))

(defn on-request-line
  [^Connection conn
   ^String line]
  (cond 
    (nil? line) (close-conn! conn)
    :else (let [parts (parse-request-line line)
                ^Request request (.request conn)]
            (cond
              (nil? parts) (close-conn! conn)
              :else (do 
                      (reset! (.uri request) (:uri parts))
                      (reset! (.method request) (:method parts))
                      (reset! (.protocol request) (:protocol parts))
                      (io/read-line! (.socket-channel conn) on-header-line conn))))))

(defn on-accept 
  [request-handler] 
  (let [^ServerSocketChannel server-socket-chan (.channel selection-key)]
    (loop [^SocketChannel socket-channel (.accept server-socket-chan)]
      (when socket-channel
        (let [socket (.socket socket-channel)
              conn (empty-connection socket-channel request-handler)]
          (.setTcpNoDelay socket false)
          (.setSoLinger socket false 0)
          (.setKeepAlive socket true)
          (.setReuseAddress socket true)
          (.configureBlocking socket-channel false)
          (io/read-line! socket-channel on-request-line conn))
          (recur (.accept server-socket-chan))))))


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
  #(let [^Request request (.request #^Connection %)
          uri-info (parse-uri @(.uri request))
          {:keys [path]} uri-info]
      (loop [routes0 routes]
        (if (empty? routes0)
          (not-found! %)
          (let [[[pat-method pat-uri handler] & t] routes0]
            (if (or (= pat-method :any) 
                    (= (name pat-method) @(.method request)))
              (let [[match & match-args] (re-find0 pat-uri path)]
                (if match 
                  (apply handler % match-args)
                  (recur t)))
              (recur t)))))))

(defn listen!
  "Listen for connections"
  [^ServerSocketChannel server-socket-channel 
   handler]
  (reactor/on-acceptable! server-socket-channel on-accept handler))

(defn serve!
  [opts 
   & routes]
  (let [{:keys [listen-backlog ports] 
         :or {listen-backlog 100 ports [8080]}} opts
        channel (bind! ports listen-backlog)]
      (spread 
        (with-reactor 
          (listen! channel (with-routes routes))
          (react)))))

(defn write-file-channel!
  [^Connection conn
   ^FileChannel channel 
   offset 
   len]
  (loop [offset (max offset 0)
         len (max len 0)]
    (if (> len 0)
      (let [amt (safe -1 (.transferTo channel offset len (.socket-channel conn)))]
        (cond
          (< amt 0) (close-conn! conn)
          (= amt 0) (reactor/on-writable-once! (.socket-channel conn) write-file-channel! conn channel offset len)
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
                range-header (get-header @(.headers #^Request (.request conn)) "Range")
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




