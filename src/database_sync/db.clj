(ns database-sync.db
  (:gen-class)
  (:require [clojure.string :as str]))


(def modelDB
  {:url "jdbc:mysql://localhost/model_db"
   :user "user"
   :pass "user"
   :name "ModelDB"})

(def workingDB
  {:url "jdbc:mysql://localhost/working_db"
   :user "user"
   :pass "user"
   :name "WorkingDB"})


(defn db-connect
  [db_info]
  (java.sql.DriverManager/getConnection (get db_info :url) (get db_info :user) (get db_info :pass)) )


(defn db-query [conn sql & params]
  (let [stmt (. conn prepareStatement sql)]
    (if (some? params)
      (doseq [i (range 1 (inc (count params)))]
        (. stmt setObject i (nth params (dec i))) ) )
    
    (. stmt executeQuery) ) )


(defn db-update [conn sql & params]
  (let [stmt (. conn prepareStatement sql)]
    (if (some? params)
      (doseq [i (range 1 (inc (count params)))]
        (. stmt setObject i (nth params (dec i))) ) )
    
    (. stmt executeUpdate) ) )


(defn db-close [conn]
  (try
    (. conn close)
    (catch Throwable t)) )
