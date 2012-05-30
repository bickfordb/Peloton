(ns peloton.reactor
  (:import java.net.InetSocketAddress)
  (:import java.nio.channels.ServerSocketChannel)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.spi.SelectorProvider)
  (:import java.util.PriorityQueue)
  (:use peloton.util))

(def ^:dynamic ^Selector selector)
(def ^:dynamic ^java.util.PriorityQueue *pending*)
(def ^:dynamic ^SelectionKey selection-key)

(defmacro with-reactor [& xs]
  `(binding [selector (until (safe nil (.openSelector (SelectorProvider/provider))))
             *pending* (PriorityQueue.)] 
     ~@xs))

(defmacro later
  [seconds & xs]
  `(let [ts# (+ (System/currentTimeMillis) (* ~seconds 1000.0))
         evt# [ts# (fn [] ~@xs)]]
     (.add *pending* evt#)))

(defn run-pending-events
  []
  (loop [evt (.peek *pending*)]
    (when (not (nil? evt))
      (let [[timestamp f] evt]
        (when (<= timestamp (System/currentTimeMillis))
          (let [] 
            (f)
            (.remove *pending*)
            (recur (.peek *pending*))))))))

(defn react 
  []
  (while true 
    (.select selector 1000) 
    (run-pending-events)
    (doseq [a-selection-key (.selectedKeys selector)]
      (binding [selection-key a-selection-key]
        (let [[f & xs] (.attachment a-selection-key)]
            (apply f xs))))))

