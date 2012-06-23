(ns peloton.example.reload
  (:gen-class)
  (:use peloton.httpd))

(defn handler
  [conn]
  (send-html! conn "hi"))

(defn -main
  [& args]
  (serve-symbol! 'peloton.example.reload/handler))

