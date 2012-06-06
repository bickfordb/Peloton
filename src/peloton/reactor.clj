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

(defn p-queue
  []
  (java.util.PriorityQueue. 
    10
    (comparator (fn [[a & _] [b & _]] 
                  (with-stderr
                    (println "compare" a b))
                  (compare a b)))))

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

(defn react 
  []
  (while true 
    (.select selector 1000) 
    (run-pending-events)
    (doseq [a-selection-key (.selectedKeys selector)]
      (binding [selection-key a-selection-key]
        (let [[f & xs] (.attachment a-selection-key)]
          (try 
            (apply f xs)
            (catch Exception e (.printStackTrace e))))))))

(defn register
  [^SelectableChannel selectable s-key f & fargs]
  (.register selectable selector (int s-key) (cons f fargs)))

