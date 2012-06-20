(ns peloton.example.blog
  (:require [hiccup.core :as hiccup])
  (:require [peloton.httpd :as httpd])
  (:require [peloton.mongo :as mongo])
  (:use peloton.fut)
  (:use peloton.util))

(defn create-mongo-conn
  []
  (mongo/connect-fut! "127.0.0.1" 27017))

(defn on-index-post
  [conn]
  (let [post-data (httpd/post-data-map conn)
        {:keys [title body]} post-data]
    (dofut [[mongo-conn] (create-mongo-conn)
            [inserted?] (when (and mongo-conn
                                   (> (count body) 0)
                                   (> (count title) 0))
                          (mongo/insert-fut! mongo-conn "blog.posts" [{:title title
                                                                      :added (java.util.Date.)
                                                                      :body body}]))]
      (httpd/see-other! conn "/"))))

(defn on-index 
  [conn]
  (dofut [[mongo-conn] (create-mongo-conn)
          [result] (mongo/query-fut! mongo-conn "blog.posts" {})
          posts (get-in result [:reply :documents])
          disconnected? (mongo/close! mongo-conn)]
         (httpd/send-html!
           conn 
           [:head]
           [:body 
            [:h1 "Blog Example"]
            [:form {:method "POST" :action "/"}  
             [:p 
              [:input {:type "text" :length "60" :name "title" :placeholder "Blog Post Title"}]]
             [:p 
              [:textarea {:cols "60" :rows "4" :name "body" :placeholder "Your Blog Body"}]]
             [:input {:type "submit" :value "Add Item"}]]
            (for [{:keys [title body]} posts]
              [:p 
               [:h4 title]
               [:p body]
               [:hr]])])))

(defn -main [& args]
  (httpd/serve! {}
          [:POST #"^/$" on-index-post]
          [:GET #"^/$" on-index]))
