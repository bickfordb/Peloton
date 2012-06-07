(ns peloton.reactor
  (:import java.net.InetSocketAddress)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.SelectableChannel)
  (:import java.nio.channels.spi.SelectorProvider)
  (:import java.util.Comparator)
  (:import java.util.PriorityQueue)
  (:use peloton.util))

(def ^:dynamic ^Selector selector)
(def ^:dynamic ^java.util.PriorityQueue *pending*)
(def ^:dynamic ^SelectionKey selection-key)

(def op-bits [SelectionKey/OP_ACCEPT SelectionKey/OP_READ SelectionKey/OP_WRITE SelectionKey/OP_CONNECT])

(defn p-queue
  []
  (java.util.PriorityQueue. 10 (comparator (fn [[a & _] [b & _]] (compare a b)))))

(defmacro with-reactor [& xs]
  `(binding [selector (until (safe nil (.openSelector (SelectorProvider/provider))))
             *pending* (p-queue)] 
     ~@xs))

(defn timeout
  [seconds f & fargs]
  (.add #^PriorityQueue *pending* 
        [(+ (System/currentTimeMillis) (* 1000.0 seconds)) f fargs]))

(defmacro later
  [seconds & body]
  `(let [f# (fn [] (do ~@body))]
     (timeout ~seconds (fn [] ~@body))))

(defn run-pending-events
  []
  (let [now (System/currentTimeMillis)]
    (loop [[timestamp f fargs] (.peek *pending*)]
      (when (and (not (nil? timestamp)) (< timestamp now))
        (.remove #^PriorityQueue *pending*)
        (try
          (apply f fargs)
          (catch Exception e
            (.printStackTrace e)))
        (recur (.peek *pending*))))))


(defmacro ignore
  [& body]
  `(try
     ~@body
     (catch Exception e# (.printStackTrace e#))))

(defn react 
  []
  (while true 
    (.select selector 1000) 
    (run-pending-events)
    (doseq [s-key (.selectedKeys selector)]
      (when (and s-key (.isValid s-key))
        (binding [selection-key s-key]
          (let [attachment (or (.attachment s-key) {})
                ^int ready (.readyOps s-key)]
            (doseq [bit op-bits]
              (when (= (bit-and ready bit) bit)
                (let [[f xs] (get attachment bit)]
                  (when f
                    (ignore (apply f xs))))))))))))

(defn register!
  [^SelectableChannel chan s-key f & fargs]
  (let [selection-key (.keyFor chan selector)
        evt (int s-key)
        callbacks {evt [f fargs]}]
    (if selection-key
      (do 
        (.attach selection-key (conj (or (.attachment selection-key) {}) callbacks))
        (.interestOps selection-key (bit-or (.interestOps selection-key) evt)))
      (.register chan selector evt callbacks))))

(defn unregister!
  [^SelectableChannel chan op]
  (let [selection-key (.keyFor chan selector)]
    (when selection-key
      (let [attachment (or (.attachment selection-key) {})
            attachment0 (dissoc attachment op)
            op0 (bit-and (.interestOps selection-key) (bit-not op))]
        (.attach selection-key attachment0)
        (.interestOps selection-key op0)))))

(defn on-op! 
  [op] 
  (fn [chan f & fargs]
    (apply register! chan op f fargs)))

(defn stop-op!
  [op]
  (fn [chan]
    (unregister! chan op)))

(defn on-once
  [chan op f fargs]
  (unregister! chan op)
  (when f
    (apply f fargs)))

(defn on-op-once!
  [op]
  (fn [chan f & fargs]
    (register! chan op on-once chan op f fargs)))

(def on-acceptable! (on-op! SelectionKey/OP_ACCEPT))
(def on-readable! (on-op! SelectionKey/OP_READ))
(def on-writable! (on-op! SelectionKey/OP_WRITE))
(def on-connectable! (on-op! SelectionKey/OP_CONNECT))

(def stop-acceptable! (stop-op! SelectionKey/OP_ACCEPT))
(def stop-readable! (stop-op! SelectionKey/OP_READ))
(def stop-writable! (stop-op! SelectionKey/OP_WRITE))
(def stop-connectable! (stop-op! SelectionKey/OP_CONNECT))

(def on-acceptable-once! (on-op-once! SelectionKey/OP_ACCEPT))
(def on-readable-once! (on-op-once! SelectionKey/OP_READ))
(def on-writable-once! (on-op-once! SelectionKey/OP_WRITE))
(def on-connectable-once! (on-op-once! SelectionKey/OP_CONNECT))

(defn reset-chan! 
  [^SelectableChannel chan]
  (let [selection-key (.keyFor chan selector)]
    (when selection-key 
      (.attach selection-key nil)
      (when (.isValid selection-key)
       (.cancel selection-key)))))
