(ns peloton.main 
  (:gen-class)
  (:use peloton.httpd)
  (:use peloton.reactor)
  (:use peloton.util))

(defonce listen-backlog 100)
(defonce num-threads 4)

(defn -main
  "main loop"
  [& ports]
  (let [ports0 (filter #(not (nil? %)) (map safe-int ports))
        channel (bind ports0 listen-backlog)
        f (fn []
            (with-reactor 
              (listen channel)
              (react)))
        threads (doseq [i (range num-threads)] (doto (Thread. f) (.start)))]
    (doseq [^Thread t threads] (.join t))))


