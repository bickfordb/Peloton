(ns peloton.test.bson
  (:use peloton.bson)
  (:import java.io.ByteArrayOutputStream)
  (:use clojure.test))

(def ex-1-doc {"hello" "world"})

(def ex-2-doc {:BSON ["awesome" 5.05 1986]})
(def ex-2-encoded [49 0 0 0 4 66 83 79 78 0 38 0 0 0 2 48 0 8 0 0 0 97 119 101 115 111 109 101 0 1 49 0 51 51 51 51 51 51 20 64 16 50 0 194 7 0 0 0 0])

(defn from-unsigned-bytes 
  [xs]
  (let [buf (ByteArrayOutputStream. )]
    (doseq [b xs]
      (.write buf b))
    (.toByteArray buf)))

(deftest encode-examples
  ;(let [expected (map #(int %) ex-2-encoded)
  ;      result (map #(int %) (encode-doc ex-1-doc))]
  ;  (is (= expected result)))

  (let [expected (apply str (map #(format " 0x%02x" %) ex-2-encoded))
        result (apply str (map #(format " 0x%02x" %) (encode-doc ex-2-doc)))]
    (is (= expected result))))

(deftest decode-examples
  (let [result (decode-doc (from-unsigned-bytes ex-2-encoded))
        expected ex-2-doc]
    (is (= expected result))))

