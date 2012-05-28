(ns peloton.main 
  (:gen-class)
  (:use peloton.httpd)
  (:use peloton.reactor)
  (:use peloton.util))

(defonce listen-backlog 100)
(defonce num-threads 4)

(defn chunked-handler 
  []
  (set-response-status! 200)
  (set-response-message! "OK")
  (add-response-header! "Content-Type" "text/html")
  (start-chunked-response!)
  (send-chunk! "<h1>hello</h1>")
  (flush-output!)
  (later-with-conn! 10.0 
    (send-chunk! "<h1>goodbye</h1>")
    (finish-chunked-response!)))

(defn index-handler
  []
  (set-response-body! "<h1>Hello from Peloton</h1>")
  (add-response-header! "Content-Type" "text/html")
  (send-response!))

(defn -main
  "main loop"
  [& ports]
  (serve!
    {:num-threads 4
     :listen-backlog 10
     :ports (filter #(not (nil? %)) (map safe-int ports))}
    [:GET #"^/$" index-handler]
    [:GET #"^/chunked$" chunked-handler]))

