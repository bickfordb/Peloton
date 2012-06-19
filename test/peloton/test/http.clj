(ns peloton.test.http
  (:require [peloton.http :as http])
  (:require [peloton.httpd :as httpd])
  (:require [peloton.fut :as fut])
  (:require [peloton.reactor :as reactor])
  (:use clojure.test))

(deftest parse-host
         (let [parsed (http/parse-url "http://www.google.com:5050/hello;bar?a-query-string#a-fragment")
               {:keys [host port protocol path fragment query]} parsed]
                (is (= protocol "http"))
                (is (= host "www.google.com"))
                (is (= port 5050))
                (is (= path "/hello"))
                (is (= query "a-query-string"))
                (is (= fragment "a-fragment"))))

(deftest header-parsing 
         (let [parsed (http/parse-response-header "HTTP/1.1 200 OK\r\nFoo: Bar\r\n\r\n")
               {:keys [status message headers]} parsed]
           (is (= status 200))
           (is (= message "OK"))
           (is (= (seq headers) [["Foo" "Bar"]]))))

(deftest test-encode-params
  (is (= (http/encode-params [[:x "1"] [:y "2"]]) "x=1&y=2")))

; Query google.
(deftest request-google
  (reactor/with-reactor
    (let [channel (httpd/bind! [54321] 100)
          routes [[:GET #"^/$" #(httpd/send-html! % [:html [:body "hi"]])]]]
      (httpd/listen! channel (httpd/with-routes routes))
      (fut/dofut [response (http/request! "http://127.0.0.1:54321")
                  {:keys [status headers body]} response]
                 (is (= status 200))
                 (is (= 
                       (String. body) 
                       "<html><body>hi</body></html>"))
                 (reactor/stop!)))))


