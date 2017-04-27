(ns insert-benchmark.core
  (:require [clj-time.core :as time]
            [clj-time.jdbc]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str])
  (:import java.io.FileReader
           java.util.UUID
           org.postgresql.copy.CopyManager
           org.postgresql.core.BaseConnection)
  (:gen-class))

(def db-spec
  {:dbtype "postgresql"
   :dbname   (System/getenv "INSERT_BENCHMARK_DBNAME")
   :host     (System/getenv "INSERT_BENCHMARK_HOST")
   :host     (System/getenv "INSERT_BENCHMARK_PORT")
   :user     (System/getenv "INSERT_BENCHMARK_USER")
   :password (System/getenv "INSERT_BENCHMARK_PASSWORD")})

(def row-count 10000)
(def thread-count 4)

(def initial-rows
  (let [now (time/now)]
    (take row-count (repeatedly #(hash-map :id (UUID/randomUUID)
                                           :created_at now
                                           :a "foo"
                                           :b 555
                                           :c 11.0)))))

(def rows
  (let [now (time/now)]
    (take row-count (repeatedly #(hash-map :id (UUID/randomUUID)
                                           :created_at now
                                           :a "bar"
                                           :b -123
                                           :c 24.0)))))

(defn select-where-id-in
  [rows]
  (let [prepared (repeat (count rows) "?")
        query (str "SELECT * FROM benchmark WHERE id IN (" (str/join ", " prepared) ")")]
    (apply vector query (map :id rows))))

(defn copy-command
  "Returns the number of rows copied"
  [db rows]
  (let [filename (str "/tmp/benchmark_" (UUID/randomUUID) ".csv")
        columns [:id :created_at :a :b :c]
        headers (map name columns)
        data (mapv #(mapv % columns) rows)]
    (with-open [writer (io/writer filename)]
      (csv/write-csv writer (cons headers data)))

    (jdbc/with-db-transaction [txn db]
      (jdbc/query txn (select-where-id-in rows))
      (let [conn (jdbc/db-connection txn)
            sql (str "COPY benchmark FROM STDIN CSV HEADER")]
        (-> (CopyManager. (cast BaseConnection conn))
            (.copyIn sql (FileReader. filename)))))))

(defn bulk-insert
  [db rows]
  (jdbc/with-db-transaction [txn db]
    (jdbc/query txn (select-where-id-in rows))
    (jdbc/insert-multi! txn :benchmark rows)))

(defn single-inserts
  [db rows]
  (jdbc/with-db-connection [cnx db]
    (doseq [row rows]
      (jdbc/with-db-transaction [tnx cnx]
        (jdbc/query db ["SELECT * FROM benchmark WHERE id = ?" (:id row)])
        (jdbc/insert! db :benchmark row)))))

(defn prep-db
  [db]
  (let [sql (->> ["DROP TABLE IF EXISTS benchmark;"
                  "CREATE TABLE benchmark ("
                  "  id         uuid PRIMARY KEY,"
                  "  created_at timestamp NOT NULL,"
                  "  a          text,"
                  "  b          integer,"
                  "  c          decimal(5,2)"
                  ");"]
                 (str/join \newline))]
    (jdbc/execute! db sql)
    (jdbc/insert-multi! db :benchmark initial-rows)))

(defn -main
  [& args]

  (println "Starting single inserts on 1 thread...")
  (prep-db db-spec)
  (->> (single-inserts db-spec rows)
       time)

  (println "Starting single inserts on" thread-count "threads...")
  (prep-db db-spec)
  (->> (partition (/ row-count thread-count) rows)
       (pmap #(single-inserts db-spec %))
       doall
       time)

  (println "Starting bulk insert on one thread...")
  (prep-db db-spec)
  (->> (bulk-insert db-spec rows)
       time)

  (println "Starting bulk insert on" thread-count "threads...")
  (prep-db db-spec)
  (->> (partition (/ row-count thread-count) rows)
       (pmap #(bulk-insert db-spec %))
       doall
       time)

  (println "Starting copy command on one thread...")
  (prep-db db-spec)
  (->> (copy-command db-spec rows)
       time)

  (println "Starting copy command on" thread-count "threads...")
  (prep-db db-spec)
  (->> (partition (/ row-count thread-count) rows)
       (pmap #(copy-command db-spec %))
       doall
       time)

  (shutdown-agents))
