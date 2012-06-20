(ns peloton.example.web
  (:gen-class)
  (:use peloton.util)
  (:use [hiccup.core :only [html]])
  (:require [peloton.httpd :as httpd])
  (:require [peloton.reactor :as reactor]))

(defn on-index
  "Return a page which says \"Hello from Peloton\""
  [conn] 
  (httpd/set-content-type-html! conn)
  (httpd/set-response-body! conn (html [:html [:body [:h1 "Hello from Peloton"]]]))
  (httpd/send-response! conn))

(defn chunk-loop 
  [conn]
  (when (not (httpd/finished? conn))
    (httpd/send-chunk! 
      conn 
      (html [:script "x++; f(x);"]))
    (httpd/flush-output! conn)
    (reactor/timeout! 1.0 chunk-loop conn)))

(defn on-chunked 
  "Return a page which displays a counter which increments once per second through JSONP chunked responses"
  [conn]
  (httpd/set-content-type-html! conn)
  (httpd/start-chunked-response! conn)
  (httpd/send-chunk! 
    conn 
    (html [:html 
           [:body
             [:h1 {:id "foo"} "1"]
             [:script "
               x=0;
               function f(n) { 
                 document.getElementById(\"foo\").innerHTML = \"\" + n;
               }"]]]))
  ; flush browser buffer
  (httpd/send-chunk! conn (format "%1024s" ""))
  (httpd/flush-output! conn)
  (reactor/timeout! 1.0 chunk-loop conn))

(defn -main [ & args] 
  (httpd/serve! {:ports [8080]
                 :listen-backlog 100}
    [:GET #"^/chunked$" on-chunked]
    [:GET #"^/$" on-index]))
