(defproject insert-benchmark "1.0.0"
  :description "Benchmark different ways of inserting into postgres"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-time/clj-time "0.13.0"]
                 [org.clojure/clojure "1.8.0"]
                 [org.clojure/data.csv "0.1.3"]
                 [org.clojure/java.jdbc "0.7.0-alpha3"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.25"]
                 [postgresql/postgresql "9.3-1102.jdbc41"]]
  :main ^:skip-aot insert-benchmark.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
