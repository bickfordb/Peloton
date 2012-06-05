(ns peloton.mongo
  (:import java.io.ByteArrayInputStream)
  (:import java.io.ByteArrayOutputStream)
  (:import java.net.InetSocketAddress)
  (:import java.nio.ByteBuffer)
  (:import java.nio.ByteOrder)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.charset.Charset)
  (:import java.util.Vector)
  (:require [peloton.bits :as bits])
  (:require [peloton.bson :as bson])
  (:require [peloton.io :as io]) 
  (:require [peloton.reactor :as reactor])
  (:use peloton.fut)
  (:use peloton.util))

(def default-port 27017)
(def frame-length-size 4)
(defn bit [n] (bit-shift-left 1 n))

(defonce last-request-id (atom 1))

(defn create-request-id 
  []
  (swap! last-request-id inc))

(def op-codes {:reply 1
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

(def update-flags {:upsert? (bit 0)
                   :multi-update? (bit 1)})

(def delete-flags {:single-remove? (bit 0)})

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
          (let [conn (Connection.
                       socket-channel
                       (Vector. )
                       (atom nil))]
            (f conn)))
          (on-connect socket-channel on-connect f))
      (catch java.io.IOException e 
        (f nil)))))

(defn send-buffers0! 
  [^Connection c]
  (loop []
    (cond
      (empty? (.out-buffers c)) (reset! (.state c) nil)
      :else (let [[^ByteBuffer b f] (first (.out-buffers c))]
              (cond 
                (= (.remaining b) 0) (do 
                                       (.removeElementAt (.out-buffers c) 0)
                                       (when f 
                                         (f true))
                                       (recur))
                :else (do 
                        (condp = (safe -1 (.write (.socket-channel c) b))
                          -1 (do 
                               (when f (f false))
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
   after]
  (-> conn (.out-buffers) (.add [b after]))
  (send-buffers! conn))

(defn write-frame! 
  [^Connection c
   ^bytes b
   on-write]
  (let [buffer (ByteBuffer/allocate 4)]
    (.order buffer ByteOrder/LITTLE_ENDIAN)
    (.putInt buffer (+ (count b) 4))
    (.flip buffer)
    (write-buffer! c buffer (fn [succ?] 
                              (if succ?
                                (write-buffer! c (ByteBuffer/wrap b) on-write)
                                (on-write false))))))

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


(defn update!
  [conn collection-name selector update after & flags]
  (let [flags0 (apply hash-map flags)
        body (ByteArrayOutputStream.)]
    (put-header! body (:update op-codes)) ;header
    (bits/put-le-i32! body 0) ; ZERO
    (bits/put-cstring! body collection-name) ; collection name
    (bits/put-le-i32! body (to-flag-bits flags0 update-flags)) ; flags
    (.write body (bson/encode-doc selector)) ; the selector document (analog to the WHERE part of a SQL query) 
    (.write body (bson/encode-doc update)) ; the update statement (analog to the SET part of a SQL query) 
    (write-frame! conn (.toByteArray body) after)))

(def update-fut! (to-fut update! 4))


(defn delete! 
  [conn collection-name selector after & flags]
  (let [flags0 (apply hash-map flags)
        body (ByteArrayOutputStream.)]
    (put-header! body (:delete op-codes)) ;header
    (bits/put-le-i32! body 0) ; ZERO
    (bits/put-cstring! body collection-name) ; collection name
    (bits/put-le-i32! body (to-flag-bits flags0 delete-flags)) ; flags
    (.write body (bson/encode-doc selector)) ; the selector document (analog to the WHERE part of a SQL query) 
    (write-frame! conn (.toByteArray body) after)))

(def delete-fut! (to-fut delete! 3))


(defn flush! 
  [conn]
  (-> conn (.socket-channel) (.socket) (.getOutputStream) (.flush)))

(defn insert!
  [conn collection-name docs after]
  (if conn
    (let [body (ByteArrayOutputStream.)
          docs0 (map bson/add-object-id docs)]
      (put-header! body (:insert op-codes))
      (bits/put-le-i32! body 0)
      (bits/put-cstring! body collection-name)
      (doseq [doc docs0]
        (.write body (bson/encode-doc doc)))
      (write-frame! conn (.toByteArray body) after))
    (after false)))

(def insert-fut! (to-fut insert! 3))

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
                                         (cond 
                                           (> (.available bs) 0) (let [d (bson/read-doc! bs)]
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
  (io/read-le-i32!  
    (.socket-channel conn)
    (fn [sz] 
      (cond 
        (nil? sz) (on-reply nil)
        :else (io/read-to-buf!
                (.socket-channel conn)
                (- sz 4)
                (fn [^ByteBuffer b]
                  (.flip b)
                  (on-reply (parse-packet b))))))))

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
                   (if (not succ?)
                     (do 
                       (with-stderr 
                         (println "Failed to write!"))
                       (after false))
                     (read-packet! conn after)))]
      (put-header! bs (:query op-codes))
      (bits/put-le-i32! bs (to-flag-bits opts0 query-flags)) ; flags
      (bits/put-cstring! bs collection-name) ; name
      (bits/put-le-i32! bs offset) ; offset
      (bits/put-le-i32! bs limit) ; number to return
      (.write bs (bson/encode-doc query-doc))
      (when fs-doc
        (.write bs (bson/encode-doc fs-doc)))
      (write-frame! conn (.toByteArray bs) after0))
    (after nil)))

(def query-fut! (to-fut query! 3))

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

    (io/on-connected socket-channel on-connect f)
    (.connect socket-channel addr)))

(def connect-fut! (to-fut connect!))

