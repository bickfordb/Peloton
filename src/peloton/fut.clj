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

  Example: 
    (let-fut [[user-succ user-id] (create-user \"Brandon\" \"Bickford\")
              [bus-succ business-id] (create-business \"Gary Danko\")
              [review-succ review-id] (when (and user-id business-id) 
                                        (create-review {:user user-id 
                                                        :business business-id 
                                                        :rating 5
                                                        :comment  \"Liked it\" ))]
        (println \"result\" {:user user-id 
                             :business business-id 
    :review review-id}))

  "
  [fut-bindings & body]
  `(let-fut0 ~fut-bindings ~body))

