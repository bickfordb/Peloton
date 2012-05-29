(ns peloton.mime)

(def appliation-octet-stream "application/octet-stream")
(def common-ext-to-mime-types 
  {".clj" "application/clojure"
   ".mp3" "application/mp3"
   ".conf" "text/plain"
   ".jpg" "image/jpeg"
   ".gif" "image/gif"
   ".png" "image/png"
   ".avi" "video/avi"
   ".html" "text/html"
   ".htm" "text/html"
   ".txt" "text/plain"
   nil appliation-octet-stream})

(defn- get-ext
  [^String path] 
  (if 
    path
    (let [idx (.lastIndexOf path ".")]
      (if (>= idx 0)
        (.toLowerCase (.substring path idx))
        "")) 
    ""))

(defn guess-mime-type-of-path 
  "Guess the mime-type of a path"
  [path ext-mapping] 
  (get ext-mapping (get-ext path) (get ext-mapping nil)))

