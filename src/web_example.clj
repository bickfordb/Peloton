(ns web-example
  (:gen-class)
  (:use peloton.util)
  (:require [peloton.httpd :as httpd])
  (:require [peloton.reactor :as reactor]))

(defn on-index
  "Return a page which says \"Hello from Peloton\""
  [conn] 
  (httpd/set-content-type-html! conn)
  (httpd/set-response-body! conn "<h1>Hello from Peloton</h1>")
  (httpd/send-response! conn))

(defn chunk-loop 
  [conn]
  (when (not (httpd/finished? conn))
    (httpd/send-chunk! 
      conn 
      (format "<script>x++;document.getElementById(\"foo\").innerHTML = \"\" + x;</script>"))
    (httpd/flush-output! conn)
    (reactor/timeout! 1.0 chunk-loop conn)))

(defn on-chunked 
  "Return a page which displays a counter which increments once per second through JSONP chunked responses"
  [conn]
  (httpd/set-content-type-html! conn)
  (httpd/start-chunked-response! conn)
  (httpd/send-chunk! conn "<script>x=0;</script><h1 id=foo>1</h1>")
  (httpd/send-chunk! conn (format "%1024s" "")) ; I read on the internet that 1K tends to escape browser buffer boundaries
  (httpd/flush-output! conn)
  (reactor/timeout! 1.0 chunk-loop conn))

(defn -main [ & args] 
  (httpd/serve! {:ports [8080]
                 :listen-backlog 100}
    [:GET #"^/chunked$" on-chunked]
    [:GET #"^/$" on-index]))
