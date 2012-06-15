Peloton
=======

Peloton is a Clojure library to simplify the creation of high performance web applications.  

Included are the following features: 

* IO reactor `peloton.reactor`
  * Timeouts

  ```clojure
  (later 1.0 
    (print "hi")) ; Print "hi" after one second
  ``` 

  * Easy-to-use IO registration 
  
  ```clojure
  (on-readable! some-socket-channel #(print "some-socket-channel is ready to read!"))
  ```

* HTTP daemon `peloton.httpd`
  * Chunked transfer encoding
  * Regex URI dispatch
  * Partial file transfers / Range header support
  * Sendfile/Zero-copy file transfers
* MongoDB client `peloton.mongo`
* Primitives and macros which writing fast asynchronous code simple (`peloton.fut` `peloton.stream`)
  * `dofut` a macro to make complex twisted asynchronous/future functions look head-first and simple.

  ```clojure
   (ns myprogram 
     (:use peloton.fut)
     (:use peloton.reactor))

   (defn load-a 
     []
     (let [f (fut)]
       (reactor/later 1.0 
         (f 1)) ; respond with 1 after 1 second
       f))

   (defn load-b 
      []
      (let [f (fut)]
        (reactor/later 2.0
          (f 2)) ; respond with 2 after 2 seconds
        f))

   (defn program
     []
     (reactor/with-reactor
       (dofut [a (load-a)  
               b (load-b)]
         (println (+ a b)) ; print the result of "load-a + load-b" (this will execute after 2 seconds)
         (reactor/stop)))) 
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
    (reactor/timeout 1.0 chunk-loop conn)))

(defn on-chunked 
  "Return a page which displays a counter which increments once per second through JSONP chunked responses"
  [conn]
  (httpd/set-content-type-html! conn)
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

httpd Benchmark
---------------

Below is a small benchmark demonstrating that a "Hello World"-ish handler can handle fifty concurrent requests and fulfill requests more than 10000 times per second.

Example request/response

```
[bran@bathysphere peloton (master)]$ curl -v http://127.0.0.1:8080/
* About to connect() to 127.0.0.1 port 8080 (#0)
*   Trying 127.0.0.1... connected
* Connected to 127.0.0.1 (127.0.0.1) port 8080 (#0)
> GET / HTTP/1.1
> User-Agent: curl/7.21.1 (x86_64-apple-darwin10.4.0) libcurl/7.21.1 OpenSSL/1.0.1c zlib/1.2.7 libidn/1.22
> Host: 127.0.0.1:8080
> Accept: */*
> 
< HTTP/1.1 200 OK
< Server: peloton/-inf
< Date: Fri, 15 Jun 2012 17:02:53 GMT
< Content-Type: text/html
< Content-Length: 27
< 
* Connection #0 to host 127.0.0.1 left intact
* Closing connection #0
<h1>Hello from Peloton</h1>
```

Send requests (concurrency=50, N=100000)

```
[bran@bathysphere peloton (master)]$ ab -n 100000 -c 50 http://127.0.0.1:8080/
This is ApacheBench, Version 2.3 <$Revision: 1178079 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking 127.0.0.1 (be patient)
Completed 10000 requests
Completed 20000 requests
Completed 30000 requests
Completed 40000 requests
Completed 50000 requests
Completed 60000 requests
Completed 70000 requests
Completed 80000 requests
Completed 90000 requests
Completed 100000 requests
Finished 100000 requests


Server Software:        peloton/-inf
Server Hostname:        127.0.0.1
Server Port:            8080

Document Path:          /
Document Length:        27 bytes

Concurrency Level:      50
Time taken for tests:   9.766 seconds
Complete requests:      100000
Failed requests:        0
Write errors:           0
Total transferred:      15000000 bytes
HTML transferred:       2700000 bytes
Requests per second:    10239.41 [#/sec] (mean)
Time per request:       4.883 [ms] (mean)
Time per request:       0.098 [ms] (mean, across all concurrent requests)
Transfer rate:          1499.91 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    2   2.3      2      64
Processing:     0    3   4.9      2     146
Waiting:        0    3   4.8      2     145
Total:          1    5   5.4      5     146

Percentage of the requests served within a certain time (ms)
  50%      5
  66%      5
  75%      5
  80%      5
  90%      5
  95%      5
  98%      6
  99%     12
 100%    146 (longest request)
```

Benchmark system:

2010 MacBook Pro, 8GB memory, SATA HDD

(I expect this will be faster on Linux on newer server class hardware)

License
-------

Apache 2.0. See LICENSE 
