(defproject jepsen.rqlite "0.1.0-SNAPSHOT"
  :description "Testing rqlite with Jepsen"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :main jepsen.rqlite
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.6"]
                 [clj-http "3.12.3"]
                 [cheshire "5.10.2"]]
  :repl-options {:init-ns jepsen.rqlite})
