(ns peloton.mongo
  (:import java.io.ByteArrayInputStream)
  (:import java.io.ByteArrayOutputStream)
  (:import java.net.InetSocketAddress)
  (:import java.nio.ByteBuffer)
  (:import java.nio.ByteOrder)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SocketChannel)
  (:import java.util.Vector)
  (:require [peloton.bits :as bits])
  (:require [peloton.bson :as bson])
  (:require [peloton.io :as io]) 
  (:require [peloton.reactor :as reactor])
  (:use peloton.fut)
  (:use peloton.stream)
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
               :get-more 2005
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
                              (reactor/on-writable-once! (.socket-channel c) send-buffers0! c))
                          (recur))))))))

(defn send-buffers! 
  [^Connection c]
  (when (not (= @(.state c) :sending))
    (reset! (.state c) :sending)
    (send-buffers0! c)))

(defn write-buffer! 
  [^Connection conn 
   ^ByteBuffer b]
  (let [f (fut)]
    (if conn 
      (do 
        (-> conn (.out-buffers) (.add [b f]))
        (send-buffers! conn)
        )
      (f false))
    f))

(defn write-frame! 
  [^Connection c
   ^bytes b]
  (let [buffer (ByteBuffer/allocate 4)]
    (.order buffer ByteOrder/LITTLE_ENDIAN)
    (.putInt buffer (+ (count b) 4))
    (.flip buffer)
    (write-buffer! c buffer)
    (write-buffer! c (ByteBuffer/wrap b))))

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
  [conn collection-name selector update & flags]
  (let [flags0 (apply hash-map flags)
        body (ByteArrayOutputStream.)]
    (put-header! body (:update op-codes)) ;header
    (bits/put-le-i32! body 0) ; ZERO
    (bits/put-cstring! body collection-name) ; collection name
    (bits/put-le-i32! body (to-flag-bits flags0 update-flags)) ; flags
    (.write body (bson/encode-doc selector)) ; the selector document (analog to the WHERE part of a SQL query) 
    (.write body (bson/encode-doc update)) ; the update statement (analog to the SET part of a SQL query) 
    (write-frame! conn (.toByteArray body))))

(defn delete! 
  [conn collection-name selector & flags]
  (let [flags0 (apply hash-map flags)
        body (ByteArrayOutputStream.)]
    (put-header! body (:delete op-codes)) ;header
    (bits/put-le-i32! body 0) ; ZERO
    (bits/put-cstring! body collection-name) ; collection name
    (bits/put-le-i32! body (to-flag-bits flags0 delete-flags)) ; flags
    (.write body (bson/encode-doc selector)) ; the selector document (analog to the WHERE part of a SQL query) 
    (write-frame! conn (.toByteArray body))))

(defn flush! 
  [conn]
  (-> conn (.socket-channel) (.socket) (.getOutputStream) (.flush)))

(defn insert!
  [conn collection-name docs]
  (let [body (ByteArrayOutputStream.)
        docs0 (map bson/add-object-id docs)]
    (put-header! body (:insert op-codes))
    (bits/put-le-i32! body 0)
    (bits/put-cstring! body collection-name)
    (doseq [doc docs0]
      (.write body (bson/encode-doc doc)))
    (write-frame! conn (.toByteArray body))))

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
                            {:flags flags
                             :cursor-id cursor-id
                             :starting-from starting-from
                             :number-returned number-returned
                             :documents docs})
        {}))))

(defn read-frame!
  [conn]
  (let [on-frame (fut)]
    (io/read-le-i32!  
      (.socket-channel conn)
      (fn [sz] 
        (cond 
          (nil? sz) (on-frame nil)
          :else (io/read-to-buf!
                  (.socket-channel conn)
                  (- sz 4)
                  (fn [^ByteBuffer b]
                    (.flip b)
                    (on-frame b))))))
    on-frame))

(defn read-packet! 
  "Read a packet

  Returns a future which fires when the packet fires.
  "
  [conn]
  (dofut [packet (read-frame! conn)] 
         (when packet (parse-packet packet))))

(defn query-raw! 
  [conn 
   collection-name 
   query-doc 
   & opts]
  (let [opts0 (apply hash-map opts)
        offset (get opts0 :offset 0)
        limit (get opts0 :limit 0)
        fieldset (get opts0 :fieldset nil)
        bs (ByteArrayOutputStream.)
        flags 0
        fs-doc (when fieldset
                 (reduce conj (for [f fieldset] {f 1})))]
    (put-header! bs (:query op-codes))
    (bits/put-le-i32! bs (to-flag-bits opts0 query-flags)) ; flags
    (bits/put-cstring! bs collection-name) ; name
    (bits/put-le-i32! bs offset) ; offset
    (bits/put-le-i32! bs limit) ; number to return
    (.write bs (bson/encode-doc query-doc))
    (when fs-doc
      (.write bs (bson/encode-doc fs-doc)))
    (dofut [query-sent? (write-frame! conn (.toByteArray bs))
            packet (when query-sent? (read-packet! conn))]
        packet)))

(defn kill-cursors!
  [conn 
   cursor-ids]
   (let [bs (ByteArrayOutputStream.)]
     (put-header! bs (:kill-cursors op-codes))
     (bits/put-le-i32! bs 0) ; zero
     (bits/put-le-i32! bs (count cursor-ids)) ; number of cursor ids
     (doseq [cursor-id cursor-ids]
       (bits/put-le-i64! bs cursor-id))
     (write-frame! conn (.toByteArray bs))))

(defn get-more!
  [conn ; the mongo collection
   collection-name ; the collection name
   number-to-return ; the number of documents to return 
   cursor-id] 
  (let [bs (ByteArrayOutputStream.)]
    (put-header! bs (:get-more op-codes))
    (bits/put-le-i32! bs 0) ; zero
    (bits/put-cstring! bs collection-name) ; name
    (bits/put-le-i32! bs number-to-return) ; number to return
    (bits/put-le-i32! bs cursor-id) ; cursor id
    (dofut [wrote? (write-frame! conn (.toByteArray bs))
            packet (when wrote? (read-packet! conn))]
           packet)))

(defn query-rest! 
  [conn collection cursor-id & opts]
  (dofut [opts- (apply hash-map opts)
          {:keys [batch-size] :or {batch-size 1000}} opts-
          more (when (and cursor-id (> cursor-id 0))
                  (get-more! conn collection batch-size cursor-id))
          {new-cursor-id :cursor-id 
           documents :documents
           :or {new-cursor-id 0 documents []}} more
          killed? (when (not (= new-cursor-id cursor-id))
                    (kill-cursors! conn [cursor-id]))
          rest- (when (and new-cursor-id (> new-cursor-id 0) (> (count documents) 0))
                  (apply query-rest! conn collection new-cursor-id opts))
          rest-documents (:documents rest)]
         (concat [] documents rest-documents)))

(defn query-stream-rest! 
  [conn collection cursor-id & opts]
  (let [^peloton.stream.IStream a-stream (stream)
        {:keys [batch-size] :or {batch-size 1000}} opts]
    (if (and cursor-id (> cursor-id 0))
      (dofut [{new-cursor-id :cursor-id 
               documents :documents
               :or {new-cursor-id 0 documents []}} (get-more! conn collection batch-size cursor-id)
              _ (when (not (= new-cursor-id cursor-id))
                        (kill-cursors! conn [cursor-id]))
              _ (doseq [document documents] (a-stream document))
              sub-stream (apply query-stream-rest! conn collection new-cursor-id opts)
              _ (do-stream [document sub-stream]
                           (a-stream document))]
             (.close! a-stream))
        (.close! a-stream))
    a-stream))
     
(defn query-stream!
  [conn collection query-doc & opts]
  (let [^peloton.stream.IStream a-stream (stream)]
    (dofut [query-result (apply query-raw! conn collection query-doc opts)
            _ (doseq [document (:documents query-result)]
                (a-stream document))
            sub-stream (apply query-stream-rest! conn collection (:cursor-id query-result) opts)
            stream-done? (do-stream [i sub-stream]
                                    (a-stream i))]
           (.close! a-stream))
    a-stream))

(defn query-all!
  [conn collection query-doc & opts]
  (dofut [ret (apply query-raw! conn collection query-doc opts)
          docs (apply query-rest! conn collection (:cursor-id ret) opts)]
          (concat [] (:documents ret) docs)))
        
(defn connect! 
  [^String address 
   ^long port]
  (let [addr (InetSocketAddress. address port)
        socket-channel (SocketChannel/open)
        ret (fut)
        socket (.socket socket-channel)]
    (.configureBlocking socket-channel false)
    (doto (.socket socket-channel)
      (.setKeepAlive true)
      (.setReuseAddress true)
      (.setSoLinger false 0)
      (.setSoTimeout 0)
      (.setTcpNoDelay false))
    (reactor/on-connectable-once! socket-channel on-connect ret)
    (.connect socket-channel addr)
    ret))

(defn close! 
  [^Connection c]
  (.close (.socket-channel c))
  (reactor/reset-chan! (.socket-channel c)))

