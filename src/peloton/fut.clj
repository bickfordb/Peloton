(ns peloton.fut
  "Fancy asynchronous future library
  
  This is named \"fut\" instead of \"future\" to avoid conflicting with the built-in future blocking/synchronous future support
  "
  )

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
      (apply h promised))
    (reset! (.promised fut) [])))

(defn subscribe!
  "Subscribe to a future"
  [^Fut fut 
   ^clojure.lang.IFn f]
  (if @(.done fut)
    (apply f @(.promised fut))
    (swap! (.listeners fut) conj f)))

(defn fut? 
  "Check to see if x is a future"
  [x] 
  (instance? Fut x))

(defmacro dofut0
  [bindings body]
  (if (empty? bindings)
    `(do ~@body)
    (let [sym (first bindings)
          fut (second bindings)
          t (nthrest bindings 2)]
      `(let [f# ~fut]
         (if (fut? f#)
           (subscribe! f#
                       (fn [ & xs# ] 
                         (let [~sym xs#]
                           `~(dofut0 ~t ~body)
                           )))
           (let [~sym f#] 
             `~(dofut0 ~t ~body)))))))

(defmacro dofut
  "Execute a body when a sequence of futures are ready.

  Each binding will be executed when the future from the previous binding is delivered.
  The body will be executed when all of the futures in the bindings are delivered.  

  Example: 
    (defn create-user ^Fut [first last] ...) 
    (defn create-business ^Fut [] ...) 
    (defn create-review ^Fut [user-id business-id rating comment] ...) 

    (dofut [[user-id] (create-user \"Brandon\" \"Bickford\")
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
  `(dofut0 ~fut-bindings ~body))

(defn to-fut
  "Convert a function which takes a \"finish\" callback to a future" 
  ([f]
  (fn [ & xs] 
    (let [^Fut a-fut (fut)
          g (fn [& ys] (apply deliver! a-fut ys))]
      (apply f (concat xs [g]))
      a-fut)))
  ([f arg-idx]
   (fn [ & xs] 
    (let [^Fut a-fut (fut)
          g (fn [& ys] (apply deliver! a-fut ys))]
      (apply f (concat (take arg-idx xs) [g] (drop arg-idx xs)))
      a-fut))))



