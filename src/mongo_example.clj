(ns mongo-example
  (:gen-class)
  (:require [peloton.reactor :as reactor])
  (:use peloton.reactor)
  (:use peloton.mongo)
  (:use peloton.util)
  (:use peloton.fut))

(defn -main 
  [& args]
  (with-reactor 
    (dofut [conn (connect! "127.0.0.1" default-port)
            deleted? (delete! conn "foods.groups" {}) 
            inserted? (insert! conn "foods.groups" [{:name "Fruits"}])
            updated? (update! conn "foods.groups" {:name "Fruits"} {:color "Green"})
            docs (query-all! conn "foods.groups" {})
            _ (delete! conn "foods.groups" {})
            _ (delete! conn "x.y" {})
            large-inserted? (insert! conn "x.y" (repeat 5000 {:x 1}))
            large-result (query-all! conn "x.y" {} :limit 5000)
            books (query-stream! conn "x.y" {})
            x (atom 0)
            _ (do-stream 
                [b books]
                true)]
           (with-stderr 
             (println "inserted?:" inserted?)
             (println "deleted?" deleted?)
             (println "updated?" updated?)
             (println "response:" docs)
             (println "other docs:" (count large-result)))
           (reactor/stop!))
    ))
