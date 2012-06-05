(ns peloton.test.bson
  (:use peloton.bson)
  (:import java.io.ByteArrayOutputStream)
  (:use clojure.test))

(def ex-2-doc {:BSON ["awesome" 5.05 1986]})
(def ex-2-encoded [49 0 0 0 4 66 83 79 78 0 38 0 0 0 2 48 0 8 0 0 0 97 119 101 115 111 109 101 0 1 49 0 51 51 51 51 51 51 20 64 16 50 0 194 7 0 0 0 0])

(defmacro is-enc-dec
  [doc]
  `(do
     (let [doc# ~doc]
      (is (= 
            doc# 
            (decode-doc (encode-doc doc#)))))))

(defn from-unsigned-bytes 
  [xs]
  (let [buf (ByteArrayOutputStream. )]
    (doseq [b xs]
      (.write buf b))
    (.toByteArray buf)))

(deftest encode-examples
  (let [expected (apply str (map #(format " 0x%02x" %) ex-2-encoded))
        result (apply str (map #(format " 0x%02x" %) (encode-doc ex-2-doc)))]
    (is (= expected result))))

(deftest decode-examples
  (let [result (decode-doc (from-unsigned-bytes ex-2-encoded))
        expected ex-2-doc]
    (is (= expected result))))

(deftest encode-doc-subdocs
         (is-enc-dec {:x {:y 5}}))

(deftest encode-doc-object-id
         (is-enc-dec {:_id (create-object-id)}))

(deftest encode-doc-int
         (is-enc-dec {:x 1}))

(deftest encode-doc-strings
         (is-enc-dec {:x "abc"}))

(deftest encode-doc-bool-true
         (is-enc-dec {:x false}))

(deftest encode-doc-bool-false
         (is-enc-dec {:x true}))

(deftest encode-decode-binary-test
         (let [doc {:a (byte-array [(byte 0x05)])}
               doc0 (decode-doc (encode-doc doc))]
           (is (java.util.Arrays/equals (:a doc) (:a doc0)))))

