(ns peloton.reactor
  (:import java.net.InetSocketAddress)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.SelectableChannel)
  (:import java.nio.channels.spi.SelectorProvider)
  (:import java.util.Comparator)
  (:import java.util.PriorityQueue)
  (:use peloton.util))

(set! *warn-on-reflection* true)

(def ^:dynamic ^SelectionKey selection-key)

(def ^:dynamic *reactor*) 

(defrecord Timeout
  [^long timestamp
   ^clojure.lang.IFn callable
   ^clojure.lang.ISeq context])

(definterface IReactor
  (start [])
  (stop [])
  (^boolean running [])
  (^java.nio.channels.Selector selector)
  (later [^long seconds 
          ^clojure.lang.IFn callable 
          ^clojure.lang.ISeq context]))

(deftype Reactor
  [^Selector selector
   ^PriorityQueue pending
   ^boolean ^{:volatile-mutable true} running?]
  IReactor 
  (stop [this] (set! running? (boolean false)))
  (start [this] (set! running? (boolean true)))
  (running [this] running?)
  (selector [this] selector)
  (later [this seconds callable context]
    (.add pending
        (Timeout. (+ (System/currentTimeMillis) (* 1000.0 seconds)) callable context))))

(defn timeout-queue
  []
  (java.util.PriorityQueue. 10 (comparator #(< #^long (.timestamp #^Timeout %1) #^long (.timestamp #^Timeout %2)))))

(defn reactor
  []
  (Reactor. 
    (until (safe nil (.openSelector (SelectorProvider/provider))))
    (timeout-queue)
    true))


(def op-bits [SelectionKey/OP_ACCEPT SelectionKey/OP_READ SelectionKey/OP_WRITE SelectionKey/OP_CONNECT])

(defn timeout
  [seconds f & fargs]
  (.later #^Reactor *reactor* seconds f fargs))

(defmacro later
  [seconds & body]
 `(.later #^Reactor *reactor* ~seconds (fn [] ~@body) ()))

(defn run-pending-events
  [^Reactor reactor]
  (let [^PriorityQueue pending (.pending reactor)]
    (loop [^Timeout timeout (.peek pending)]
      (when (and timeout (< (.timestamp timeout) (System/currentTimeMillis)))
        (.remove pending)
        (ignore-and-print
          (apply (.callable timeout) (.context timeout)))
        (recur (.peek pending))))))

(defn react 
  [^Reactor reactor]
  (while (.running reactor)
    (.select (.selector reactor) 50)
    (run-pending-events reactor)
    (doseq [^SelectionKey s-key (.selectedKeys (.selector reactor))]
      (when (and s-key (.isValid s-key))
        (binding [selection-key s-key]
          (let [attachment (or (.attachment s-key) {})
                ready (.readyOps s-key)]
            (doseq [bit op-bits]
              (when (== (bit-and ready bit)  bit)
                (let [[f xs] (get attachment bit)]
                  (when f
                    (ignore-and-print (apply f xs))))))))))))

(defmacro with-reactor [& xs]
  `(binding [*reactor* (reactor)]
      ~@xs
      (react *reactor*)))

(defn register-reactor!
  [^SelectableChannel chan #^Reactor reactor s-key f & fargs]
  (let [selector (.selector reactor)
        selection-key (.keyFor chan selector)
        evt (int s-key)
        callbacks {evt [f fargs]}]
    (if selection-key
      (do 
        (.attach selection-key (conj (or (.attachment selection-key) {}) callbacks))
        (.interestOps selection-key (bit-or (.interestOps selection-key) evt)))
      (.register chan selector evt callbacks))))

(defn register!
  [chan s-key f & fargs]
  (apply register-reactor! chan *reactor* s-key f fargs))


(defn unregister-reactor!
  [^SelectableChannel chan ^Reactor reactor op]
  (let [selector (.selector reactor)
        selection-key (.keyFor chan selector)]
    (when selection-key
      (let [attachment (or (.attachment selection-key) {})
            attachment0 (dissoc attachment op)
            op0 (bit-and (.interestOps selection-key) (bit-not op))]
        (.attach selection-key attachment0)
        (.interestOps selection-key op0)))))

(defn unregister!
  [^SelectableChannel chan op]
  (unregister-reactor! chan *reactor* op))

(defn on-op! 
  [op] 
  (fn [chan f & fargs]
    (apply register-reactor! chan *reactor* op f fargs)))

(defn stop-op!
  [op]
  (fn [chan]
    (unregister! chan op)))

(defn on-once
  [reactor chan op f fargs]
  (unregister-reactor! chan reactor op)
  (when f
    (apply f fargs)))

(defn on-op-once!
  [op]
  (fn [chan f & fargs]
    (register-reactor! chan *reactor* op on-once *reactor* chan op f fargs)))

(defn on-reactor-op-once!
  [op]
  (fn [reactor chan f & fargs]
    (register-reactor! chan reactor op on-once reactor chan op f fargs)))

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

(def on-reactor-acceptable-once! (on-reactor-op-once! SelectionKey/OP_ACCEPT))
(def on-reactor-readable-once! (on-reactor-op-once! SelectionKey/OP_READ))
(def on-reactor-writable-once! (on-reactor-op-once! SelectionKey/OP_WRITE))
(def on-reactor-connectable-once! (on-reactor-op-once! SelectionKey/OP_CONNECT))

(defn reset-chan! 
  [^SelectableChannel chan]
  (let [selection-key (.keyFor chan (.selector #^Reactor *reactor*))]
    (when selection-key 
      (.attach selection-key nil)
      (when (.isValid selection-key)
       (.cancel selection-key)))))

(defn stop!
  "Stop the reactor"
  []
  (.stop #^Reactor *reactor*))


