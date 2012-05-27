(ns peloton.reactor
  (:import java.nio.ByteBuffer)
  (:import java.net.InetSocketAddress)
  (:import java.nio.channels.ServerSocketChannel)
  (:import java.nio.channels.SocketChannel)
  (:import java.nio.channels.Selector)
  (:import java.nio.channels.SelectionKey)
  (:import java.nio.channels.spi.SelectorProvider)
  (:use peloton.util))

(defmacro with-reactor [& xs]
  `(binding [selector (.openSelector (SelectorProvider/provider))] 
     ~@xs))

(def ^:dynamic ^Selector selector)
(def ^:dynamic ^SelectionKey selection-key)

(defn react 
  []
  (while true 
    (.select selector 10000) 
    (doseq [a-selection-key (.selectedKeys selector)]
      (binding [selection-key a-selection-key]
        (let [[f & xs] (.attachment a-selection-key)]
            (apply f xs))))))
