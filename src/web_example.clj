(ns web-example
  (:gen-class)
  (:use peloton.util)
  (:require [peloton.httpd :as httpd])
  (:require [peloton.reactor :as reactor]))

(defn on-index
  [conn] 
  (httpd/set-response-body! conn "<h1>Hello from Peloton</h1>")
  (httpd/add-response-header! conn "Content-Type" "text/html")
  (httpd/send-response! conn))

(defn chunk-loop 
  [conn]
  (when (not (httpd/finished? conn))
    (httpd/send-chunk! conn 
                       (format "<script>x++;document.getElementById(\"foo\").innerHTML = \"\" + x;</script>"))
    (httpd/flush-output! conn)
    (reactor/timeout 1.0 chunk-loop conn)))

(defn on-chunked 
  [conn]
  (httpd/set-response-status! conn 200)
  (httpd/set-response-message! conn "OK")
  (httpd/add-response-header! conn "Content-Type" "text/html")
  (httpd/start-chunked-response! conn)
  (httpd/send-chunk! conn "<script>x=0;</script><h1 id=foo>1</h1>")
  (doseq [i (range 1024)]
    (httpd/send-chunk! conn " "))
  (httpd/flush-output! conn)
  (reactor/timeout 1.0 chunk-loop conn))

(defn -main [ & args] 
  (httpd/serve! {:ports [8080]
                 :listen-backlog 100}
    [:GET #"^/chunked$" on-chunked]
    [:GET #"^/$" on-index]))
