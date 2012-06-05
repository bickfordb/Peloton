(ns peloton.mongo
  (:import java.io.ByteArrayInputStream)
  (:import java.io.ByteArrayOutputStream)
  (:import java.net.InetSocketAddress)
  (:import java.nio.ByteOrder)
  (:import java.nio.ByteBuffer)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.charset.Charset)
  (:import java.util.Vector)
  (:use peloton.fut)
  (:require [peloton.bson :as bson])
  (:require [peloton.reactor :as reactor])
  (:require [peloton.bits :as bits])
  (:require [peloton.io :as io]) 
  (:use peloton.util))

(def default-port 27017)
(def frame-length-size 4)
(defn bit [n] (bit-shift-left 1 n))

(defonce last-request-id (atom 1))

(defn create-request-id 
  []
  (swap! last-request-id inc))

(def op-codes 
  {:reply 1
   :msg 1000
   :update 2001
   :insert 2002
   :query 2004
   :getmore 2005
   :delete 2006
   :kill-cursors 2007})

(def query-flags {:tailable-cursor? (bit 1)
                 :slave-ok? (bit 2)
                 :op-log-replay? (bit 3)
                 :no-cursor-timeout? (bit 4)
                 :await-data? (bit 4)
                 :exhaust? (bit 5)
                 :partial? (bit 6)})

(def response-flags {:cursor-not-found? 0
                    :query-failure? (bit 1)
                    :shard-config-state? (bit 2)
                    :await-capable? (bit 3)})

(defrecord Connection  
  [^SocketChannel socket-channel
   ^Vector out-buffers
   ^clojure.lang.Atom state])

(defn on-connect 
  [f]
  (let [^SocketChannel socket-channel (.channel reactor/selection-key)]
    (try 
      (if (.finishConnect socket-channel)
        (do 
          (with-stderr
            (println "connected"))
          (let [conn (Connection.
                       socket-channel
                       (Vector. )
                       (atom nil))]
            (f conn)))
        (with-stderr
          (on-connect socket-channel on-connect f)
          (println "failed to finish connection")))
      (catch java.io.IOException e 
        (with-stderr 
          (println "failed to connect!"))
        (f nil)))
    (when (not (.isConnected socket-channel))
      (with-stderr
        (println "retrying connection"))
      )))

(defn send-buffers0! 
  [^Connection c]
  (loop []
    (cond
      (empty? (.out-buffers c)) (reset! (.state c) nil)
      :else (let [[^ByteBuffer b f fargs] (first (.out-buffers c))]
              ;(with-stderr
              ;  (println "b" b "f" f "fargs" fargs (.remaining b)))
              (cond 
                (= (.remaining b) 0) (do 
                                       (.removeElementAt (.out-buffers c) 0)
                                       (when f 
                                         (apply f true fargs))
                                       (recur))
                :else (do 
                        ;(with-stderr (println "Writing"))
                        (condp = (safe -1 (.write (.socket-channel c) b))
                          -1 (do 
                               (apply f false fargs)
                               (recur))
                          0 (do
                              (io/on-writable (.socket-channel c) send-buffers0! c))
                          (recur))))))))

(defn send-buffers! 
  [^Connection c]
  (when (not (= @(.state c) :sending))
    (reset! (.state c) :sending)
    (send-buffers0! c)))

(defn write-buffer! 
  [^Connection conn 
   ^ByteBuffer b
   after 
   & after-args]
  
  (-> conn (.out-buffers) (.add [b after after-args]))
  (send-buffers! conn))

(defn write-frame! 
  [^Connection c
   ^bytes b
   on-write]
  (let [buffer (ByteBuffer/allocate 4)]
    (.order buffer ByteOrder/LITTLE_ENDIAN)
    (.putInt buffer (count b))
    (.flip buffer)
    (write-buffer! c buffer nib))
  (write-buffer! c (ByteBuffer/wrap b) on-write))

(defn put-header! 
  ([bs op-code]
   (put-header! bs (create-request-id) 0 op-code))
  ([^ByteArrayOutputStream bs
   ^long request-id
   ^long response-to 
   ^long op-code]
   (bits/put-le-i32! bs request-id)
   (bits/put-le-i32! bs response-to)
   (bits/put-le-i32! bs op-code)))

(def update-flags {:upsert? (bit 0)
                   :multi-update? (bit 1)})

(defn update-docs!
  [conn collection-name selector update after & flags]
  (let [flags0 (apply hash-map flags)
        body (ByteArrayOutputStream.)]
    (put-header! body (:update op-codes))
    (bits/put-le-i32! body 0) ; ZERO
    (bits/put-cstring! body collection-name)
    (bits/put-le-i32! body (to-flag-bits flags0 update-flags)) ; flags
    (.write body (bson/encode-doc selector))
    (.write body (bson/encode-doc update))
    (write-frame! conn (.toByteArray body) after)))


(defn flush! 
  [conn]
  (-> conn (.socket-channel) (.socket) (.getOutputStream) (.flush)))

(defn insert!
  [conn collection-name docs after]
  (if conn
    (do 
      (with-stderr
        (println "insert" collection-name "docs" docs))
      (let [body (ByteArrayOutputStream.)
            docs0 (map bson/add-object-id docs)]
        (put-header! body (:insert op-codes))
        (bits/put-le-i32! body 0)
        (bits/put-cstring! body collection-name)
        (doseq [doc docs0]
          (.write body (bson/encode-doc doc)))
        (write-frame! conn (.toByteArray body) after)))
    (after false)))

(def insert-fut! (to-fut insert!))

(defn parse-packet
  [^ByteBuffer b]
  (.order b ByteOrder/LITTLE_ENDIAN)
  (let [request-id (.getInt b)
        response-to (.getInt b)
        code (.getInt b)]
    (conj 
      {:header {:request-id request-id
                :response-to response-to
                :opcode code}}
      (condp = code
        (:reply op-codes) (let [flags (.getInt b)
                                cursor-id (.getLong b)
                                starting-from (.getInt b)
                                number-returned (.getInt b) 
                                docs (let 
                                       [bs (ByteArrayInputStream. (bits/slice-remaining-buffer b))]
                                       (loop [dset []]
                                         (with-stderr
                                           (println "Read docs"))
                                         (cond 
                                           (> (.available bs) 0) (let [d (bson/read-doc! bs)]
                                                                   (with-stderr 
                                                                     (println "doc" d))
                                                                   (if (nil? d) 
                                                                     (recur dset)
                                                                     (recur (conj dset d))))
                                           :else dset)))]
                            {:reply {:flags flags
                                     :cursor-id cursor-id
                                     :starting-from starting-from
                                     :number-returned number-returned
                                     :documents docs}})
        {}))))

(defn read-packet! 
  [conn on-reply]
  (with-stderr
    (println "reading packet!"))
  (io/read-le-i32!  
    (.socket-channel conn)
    (fn [sz] 
      (println "got" sz)
      (cond 
        (nil? sz) (on-reply nil)
        :else (io/read-to-buf!
                (.socket-channel conn)
                (- sz 4)
                (fn [^ByteBuffer b]
                  (.flip b)
                  (let [p (parse-packet b)]
                    (with-stderr "packet: " (println p))
                    (on-reply p))
                  ))))))

(defn query! 
  [conn 
   collection-name 
   query-doc 
   after 
   & opts]
  (if conn
    (let [opts0 (apply hash-map opts)
          offset (get opts0 :offset 0)
          limit (get opts0 :limit 0)
          fieldset (get opts0 :fieldset nil)
          bs (ByteArrayOutputStream.)
          flags 0
          fs-doc (when fieldset
                   (reduce conj (for [f fieldset] {f 1})))
          after0 (fn [succ?]
                   (with-stderr
                     (println "write: " succ?))
                   (if (not succ?)
                     (after nil)
                     (read-packet! conn after)))]
      (put-header! bs (:query op-codes))
      (bits/put-le-i32! bs 0)
      (bits/put-cstring! bs collection-name)
      (bits/put-le-i32! bs offset)
      (bits/put-le-i32! bs limit)
      (.write bs (bson/encode-doc query-doc))
      (when fs-doc
        (.write bs (bson/encode-doc fs-doc)))
      (write-frame! conn (.toByteArray bs) after0))
    (after nil)))


(def query-fut! (to-fut query!))

(defn connect! 
  [^String address 
   ^long port
   f]
  (let [addr (InetSocketAddress. address port)
        socket-channel (SocketChannel/open)
        socket (.socket socket-channel)]
    (.configureBlocking socket-channel false)
    (doto (.socket socket-channel)
      (.setKeepAlive true)
      (.setReuseAddress true)
      (.setSoLinger false 0)
      (.setSoTimeout 0)
      (.setTcpNoDelay false))

    (binding [*out* *err*]
      (println "connecting"))  
    (io/on-connected socket-channel on-connect f)
    (.connect socket-channel addr)))

(def connect-fut! (to-fut connect!))

;(declare ^:dynamic #^Connection *conn*)

;(defmacro with-conn 
;  [host port & body]
;  `(let [g# (fn [c#] 
;              (binding [*conn* c#] ~@body))]
;     (connect! ~host ~port g#)))
