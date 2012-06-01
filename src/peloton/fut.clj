(ns peloton.fut
  "Fancy asynchronous future library
  
  This is named \"fut\" instead of \"future\" to avoid conflicting with the built-in future blocking/synchronous future support
  ")

(declare subscribe!)

(defrecord Fut 
  [^clojure.lang.Atom promised
   ^clojure.lang.Atom done
   ^clojure.lang.Atom listeners]
  clojure.lang.IFn
  (invoke [this] this)
  (invoke [this & xs] (doseq [f xs] (subscribe! this f))))

(defn fut 
  "Create a future"
  [] 
  (Fut. 
    (atom [])
    (atom false)
    (atom [])))

(defn deliver! 
  "Deliver the result to a future"
  [^Fut fut 
   & promised]
  (when (not @(.done fut))
    (reset! (.done fut) true)
    (reset! (.promised fut) promised)
    (doseq [h @(.listeners fut)]
      (apply h promised))))

(defn subscribe!
  "Subscribe to a future"
  [^Fut fut 
   ^clojure.lang.IFn f]
  (if @(.done fut)
    (apply f (.values fut))
    (swap! (.listeners fut) conj f)))

(defn fut? 
  "Check to see if x is a future"
  [x] 
  (instance? Fut x))

(defmacro let-fut0
  [bindings body]
  (if (empty? bindings)
    `(do ~@body)
    (let [sym (first bindings)
          fut (second bindings)
          t (nthrest bindings 2)]
      `(if (fut? ~fut)
         (subscribe! ~fut
                     (fn [ & xs# ] 
                       (let [~sym xs#]
                         `~(let-fut0 ~t ~body)
                         )))
         (let [~sym ~fut] 
           `~(let-fut0 ~t ~body))
         ))))

(defmacro let-fut
  "Execute a body when a sequence of futures are ready.

  Each binding will be executed when the future from the previous binding is delivered.
  The body will be executed when all of the futures in the bindings are delivered.  

  Example: 
    (defn create-user ^Fut [first last] ...) 
    (defn create-business ^Fut [] ...) 
    (defn create-review ^Fut [user-id business-id rating comment] ...) 

    (let-fut [[user-id] (create-user \"Brandon\" \"Bickford\")
              [business-id] (create-business \"Gary Danko\")
              [review-id] (when (and user-id business-id) 
                                        (create-review user-id 
                                                        business-id 
                                                        5
                                                        \"Liked it\" ))]
        (println \"user:\" user-id)
        (println \"business:\" business-id)
        (println \"review:\" review-id))
  "
  [fut-bindings & body]
  `(let-fut0 ~fut-bindings ~body))

