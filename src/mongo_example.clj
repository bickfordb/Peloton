(ns mongo-example
  (:gen-class)
  (:use peloton.reactor)
  (:use peloton.mongo)
  (:use peloton.util)
  (:use peloton.fut))

(defn -main 
  [& args]
  (with-reactor 
    (dofut [[conn] (connect-fut! "127.0.0.1" default-port)
            [deleted?] (delete-fut! conn "foods.groups" {}) 
            [inserted?] (insert-fut! conn "foods.groups" [{:name "Fruits"}])
            [updated? & xs] (update-fut! conn "foods.groups" {:name "Fruits"} {:color "green"})
            [query-response] (query-fut! conn "foods.groups" {})]
           (with-stderr 
             (println "inserted?:" inserted?)
             (println "deleted?" deleted?)
             (println "updated?" updated?)
             (println "response:" query-response)))
    (react)))
