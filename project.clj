(defproject jepsen.rqlite "0.1.0-SNAPSHOT"
  :description "A Jepsen test for rqlite"
  :url "http://example.com/FIXME"
  :license {:name "The MIT License"
          :url "http://opensource.org/licenses/MIT"}
  :main jepsen.rqlite
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [jepsen "0.2.1-SNAPSHOT"]]
  :repl-options {:init-ns jepsen.rqlite})
