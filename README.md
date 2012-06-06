Peloton
=======

Peloton is a Clojure library to simplify creating web applications which use event driven / non-blocking IO. 

Included are the following features: 

* IO reactor `peloton.reactor`
  * Timeouts
* HTTP daemon `peloton.httpd`
  * Chunked transfer encoding
  * Regex URI dispatch
  * Partial file transfers / Range header support
  * Sendfile/Zero-copy file transfers
* MongoDB client `peloton.mongo`
* Asynchronous futures `peloton.fut`
 
  * `dofut` a macro to make writing twisted asynchronous/future functions look head-first.  

  ```clojure
   (ns myprogram 
     (:use peloton.fut)
     (:use peloton.reactor))

   (defn load-a 
     []
     (let [f (fut)]
       (reactor/later 1.0 
         (deliver! f 1))
       f))

   (defn load-b 
      []
      (let [f (fut)]
        (reactor/later 2.0
          (deliver! f 2))
        f))

   (defn program
     []
     (dofut [[a] (load-a)
             [b] (load-b)]
             (println (+ a b)))) ; print 3
  ```

Usage
-------

#### HTTPD
```clojure

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
```

License
-------

Apache 2.0. See LICENSE 
