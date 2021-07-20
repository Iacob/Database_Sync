(ns database-sync.main
  (:gen-class)
  (:require [database-sync.db :as db])
  (:require [clojure.set :as set]))



(defn list-tables [db_info]
  
  (defn -extract-tables-from-rs [rs]
    (let [list1 (transient [])]
      (while (. rs next)
        (conj! list1 (. rs getString 1)) )
      (persistent! list1)
      )
    )
    
  (->
   (db/db-connect db_info)
   (db/db-query "show tables")
   (-extract-tables-from-rs)
   )
  )

(defn extract-columns-from-rs [rs]
  (let [list1 (transient [])]
    (while (. rs next)
      (conj! list1 {:field (. rs getString "Field")
                    :type (. rs getString "Type")
                    :null (. rs getString "Null")
                    :default (. rs getString "Default")
                    :extra (. rs getString "Extra")
                    :comment (. rs getString "Comment")})
      )
    (persistent! list1) ) )


(defn extract-ddl-from-rs [rs]
  (if (. rs next) (. rs getString 2) nil ) )


(defn make-new-column-sql [table column lastColumn]
  (str "ALTER TABLE " table
       " ADD " (get column :field) " " (get column :type)
       (if (some? (get column :default))
         (str " default '" (get column :default) "'"))
       (if (= "NO" (get column :null))
         " NOT NULL" " NULL")
       " COMMENT '" (get column :comment) "'"
       " after " (get lastColumn :field)) )

(defn check-different-column [table modelColumn workingColumn]
  (when-not (= modelColumn workingColumn)
    (println "-- column of table " table " different " modelColumn " " workingColumn)
    (println
     (str "ALTER TABLE " table
          " MODIFY COLUMN " (get modelColumn :field) " " (get modelColumn :type)
          (if (some? (get modelColumn :default))
            (str " default '" (get modelColumn :default) "'"))
          (if (= "NO" (get modelColumn :null)) " NOT NULL" " NULL")
          " COMMENT '" (get modelColumn :comment) "'") ) )
  )


(defn create-new-tables [modelConn workingConn tablesToCreate]
  (doseq [table tablesToCreate]
    (->
     (db/db-query modelConn (str "SHOW CREATE TABLE " table))
     (extract-ddl-from-rs)
     (println) ) ) )


(defn synchronizeDBs [modelDB workingDB]
  
  (let [modelTables (list-tables modelDB)
        workingTables (list-tables workingDB)
        tables (set/intersection (set modelTables) (set workingTables))
        tablesToCreate (set/difference (set modelTables) (set workingTables))
        modelConn (db/db-connect modelDB)
        workingConn (db/db-connect workingDB)]
    
    (doseq [table tables]
      
      (let [modelColumns (extract-columns-from-rs
                          (db/db-query modelConn
                                       (str "show full columns from " table)))
            workingColumns (extract-columns-from-rs
                            (db/db-query
                             workingConn
                             (str "show full columns from " table)))]

        (doseq [[colIdx column] (map-indexed vector modelColumns)]
          (let [workingColumn
                (first (filter #(= (get %1 :field) (get column :field))
                               workingColumns)) ]
            (if (nil? workingColumn)
              (do
                (println "-- new column for table: " table)
                (println "-- " column)
                (let [lastColumn (if (> colIdx 0)
                                (nth modelColumns (dec colIdx))
                                nil)]
                  (println (make-new-column-sql table column lastColumn)) ) )
              (check-different-column table column workingColumn)
              )
            )
          ;; (if (some #(= (get column :field) (get %1 :field)) workingColumns)
          ;;   (check-different-column table column modelColumns workingColumns)
          ;;   (do
          ;;     (println "-- new column for table: " table)
          ;;     (println "-- " column)
          ;;     (println (make-new-column-sql table column)) ) )
          )

        )
      
      )

    (create-new-tables modelConn workingConn tablesToCreate)

    
    )


  
  
  )


(defn -main
  "main function"
  [& args]

  (let [workingDBs [db/workingDB]]
    (doseq [workingDB workingDBs]
      (println)
      (println "--------------------------------------------------")
      (println "--------------------------------------------------")
      (println "--------------------------------------------------")
      (println "----- Synchronizing Database: " (get workingDB :name))
      (println "--------------------------------------------------")
      (println "--------------------------------------------------")
      (println "--------------------------------------------------")
      (println)
      (synchronizeDBs db/modelDB workingDB) ) )

  )
