(ns peloton.example.mongo
  (:gen-class)
  (:require [peloton.reactor :as reactor])
  (:use peloton.reactor)
  (:use peloton.mongo)
  (:use peloton.util)
  (:use peloton.stream)
  (:use peloton.fut))

(defn println-err
  [& s]
  (binding [*out* *err*]
    (apply println s)))

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
            books (query-stream! conn "x.y" {} :limit 5000)
            x (atom 0)
            stream-done? (do-stream [b books]
                                    (swap! x inc))]
           (with-stderr 
             (println-err "inserted?:" inserted?)
             (println-err "deleted?" deleted?)
             (println-err "updated?" updated?)
             (println-err "response:" docs)
             (println-err "other docs:" (count large-result))
             (println-err "books:" books)
             (println-err "stream-done?" stream-done?)
             (println-err "other docs2:" @x))
              

           (reactor/stop!))))
