(ns peloton.cell)

(defprotocol ICell
  (bind! [cell listener])
  (unbind! [cell listener]))

(defn cell? 
  [x]
  (satisfies? ICell x))

