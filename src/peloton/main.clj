(ns peloton.main 
  (:gen-class)
  (:use peloton.httpd)
  (:require clojure.string)
  (:use peloton.reactor)
  (:use peloton.util))

(defonce listen-backlog 100)
(defonce num-threads 4)

(defn chunk-loop 
  []
  (later-with-conn! 
    1.0
    (send-chunk! 
      (format "<script>x++;document.getElementById(\"foo\").innerHTML = \"\" + x;</script>"))
    (chunk-loop)))

(defn chunked-handler 
  []
  (set-response-status! 200)
  (set-response-message! "OK")
  (add-response-header! "Content-Type" "text/html")
  (start-chunked-response!)
  (send-chunk! "<script>x=0;</script><h1 id=foo>1</h1>")
  (doseq [i (range 1024)]
    (send-chunk! " "))
  (flush-output!)
  (chunk-loop))

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
    [:GET #"^/etc/(.+)$" (create-file-handler "/etc/")]
    [:GET #"^/chunked$" chunked-handler]))

