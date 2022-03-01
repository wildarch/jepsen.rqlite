(defproject jepsen.rqlite "0.1.0-SNAPSHOT"
  :description "A Jepsen test for rqlite"
  :url "http://example.com/FIXME"
  :license {:name "The MIT License"
            :url  "http://opensource.org/licenses/MIT"}
  :main jepsen.rqlite.sequential
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [jepsen "0.2.6"]
                 [http-kit "2.5.3"]
                 [com.github.rqlite/rqlite-java "master"]
                 [cheshire "5.10.2"]]
  ; For some weird reason, this fails the first time. 
  ; Rerun and it will work!
  :repositories [["jitpack" "https://jitpack.io"]]
  :repl-options {:init-ns jepsen.rqlite.sequential})
