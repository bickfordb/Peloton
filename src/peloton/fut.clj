(ns peloton.fut
  "Fancy asynchronous future library
  
  This is named \"fut\" instead of \"future\" to avoid conflicting with the built-in future blocking/synchronous future support
  "
  (:require clojure.walk)
  (:use peloton.util) 
  )

(defprotocol ICell
  (bind! [fut listener])
  (unbind! [fut listener])
  ; from IFn
  (invoke [fut a-val]))

(defrecord ChannelCell
 [^clojure.lang.Atom buffer
  ^clojure.lang.Atom done
  ^clojure.lang.Atom listener]
  ICell
  (bind! [this l]
    (reset! listener l)
    (if l 
      (while (and (not @done) (not (empty? @buffer)))
        (swap! @buffer (fn [xs]
                         (l (first xs))
                         (rest xs))))))
  (unbind! [this l]
    (swap! listener (fn [x] (cond (= x l) nil :else x))))
  clojure.lang.IFn
  (invoke [this val] 
    (let [l @listener]
      (if l 
        (l val)
        (reset! buffer conj val)))))

; a cell which only fires once.
(defrecord OneShotCell
  [^clojure.lang.Atom promised
   ^clojure.lang.Atom done
   ^clojure.lang.Atom listeners]
  ICell
  (bind! [this listener]
    (if @done
      (listener @promised)
      (swap! listeners conj listener))
    nil)
  (unbind! [this listener] 
    (if @done
      (listener @promised)
      (swap! listeners (fn [xs] (remove #(= %1) xs))))
    nil)
  clojure.lang.IFn
  (invoke [this val] 
    (when (not @done)
      (reset! promised val)
      (reset! done true)
      ; send the message
      (doseq [listener @listeners]
        (listener val))
      ; clear existing listeners
      (reset! listeners [])
      nil)))

(defn fut 
  "Create a future"
  [] 
  (OneShotCell. 
    (atom [])
    (atom false)
    (atom [])))

(defn fut? 
  "Check to see if x is a future"
  [x] 
  (satisfies? ICell x))

(defn to-fut
  "Convert a function which takes a \"finish\" callback to a future" 
  ([f]
   (fn [& xs] 
     (let [^Fut a-fut (fut)
           g (fn [y & ys] (if ys 
                            (a-fut (conj y ys))
                            (a-fut y)))]
           (apply f (concat xs [g]))
           a-fut)))
  ([f arg-idx]
   (fn [ & xs] 
    (let [^Fut a-fut (fut)
          g (fn [y & ys] (if ys
                           (a-fut (conj y ys))
                           (a-fut y)))]
      (apply f (concat (take arg-idx xs) [g] (drop arg-idx xs)))
      a-fut))))

(defn do-fut-inner-body
  [outer-fut body]
  (list outer-fut (cons `do body)))

(defn do-fut-inner-bind 
  [bind-to bind-from-fut on-fut]
  (let [bind-from-fut0 (gensym "bind-from-fut-")]
    (list 'let [bind-from-fut0 bind-from-fut]
          (list 'if (list `fut? bind-from-fut0)
                (list '.bind! bind-from-fut0 (list 'fn [bind-to] on-fut))
                (list `let [bind-to bind-from-fut0] on-fut)))))

(defn do-fut-inner 
  [bindings outer-fut body]
    (cond 
      (empty? bindings) (do-fut-inner-body outer-fut body)
      :else (let [[bind-to bind-from-fut & tail] bindings
                  rec (do-fut-inner tail outer-fut body)]
              (do-fut-inner-bind bind-to bind-from-fut rec))))

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

  [bindings & body]
  (let [ret-fut-sym (gensym "retfut")
        dofut-body (do-fut-inner bindings ret-fut-sym body)]
    (list 'let [ret-fut-sym (list `fut)] 
          dofut-body 
          ret-fut-sym)))

(defn future-ref?
  [form]
  (and (symbol? form) 
       (.startsWith (name form) "?")))

(declare replace-future-ref)

(defn replace-future-ref-list
  [a-form] 
  (loop [before () 
         t a-form]
    (cond 
      (empty? t) a-form ; we couldn't find any replacements
      :else (let [[h & t0] t]
              (cond 
                (future-ref? h) (let [bind-to (gensym "fut")
                                      bind-from (symbol (.substring (name h) 1))
                                      replaced (list 'dofut [bind-to bind-from]
                                                     (replace-future-ref-list 
                                                       (concat before (list bind-to) t0)))]
                                  replaced)
                :else (recur (concat before (list h)) t0))))))

(defn replace-future-ref
  [a-form]
  (cond 
    (list? a-form) (replace-future-ref-list a-form)
   
    :else a-form))

(defn replace-future-sym
  [a-sym]
  (let [bind-to (gensym "fut")
        bind-from (symbol (.substring (name a-sym) 1))]
    (list 'dofut [bind-to bind-from] bind-to)))

(defmacro >?
  [form]
  (cond 
    (future-ref? form) (replace-future-sym form) 
    :else (clojure.walk/postwalk replace-future-ref form)))

;(defmacro >>= 
;  "Bind futures together"
;  [h-form & t-form]
;  `(let [h-form0# ~h-form]
;     (if (fut? h-form0#)
;       (.bind! h-form0# (fn [x#] (-> x# ~t-form)))
;       (-> h-form0# ~t-form))))
;
;(defmacro >>
;  "Thread values through function(s) which return futures.
;
;  Usage. (>= (connect) (fn [conn] (query conn {:id 5})) (render-page )))
;  "
;  [h-form t-form]
;  `(let [h-form0# ~h-form]
;     (if (fut? h-form0#)
;       (.bind! h-form0# (fn [x#] ~t-form)))
;       ~t-form))
;
