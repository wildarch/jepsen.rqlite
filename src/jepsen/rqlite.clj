(ns jepsen.rqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [control :as c]
                    [db :as db]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(def dir "/opt/rqlite")
(def binary "rqlite")
(def logfile (str dir "/rqlite.log"))
(def pidfile (str dir "/rqlite.pid"))

(defn node-addr
  "An address for connecting to a node on a particular port."
  [node port]
  (str node ":" port))

(defn raft-addr
  "The raft address for the node."
  [node]
  (node-addr node 4002))

(defn http-addr 
  "The HTTP url clients use to talk to a node."
  [node]
  (node-addr node 4001))

(defn initial-cluster
  "Constructs an initial cluster string for a test, like
  \"foo=foo:4002,bar=bar:4002,...\""
  [test]
  (->> (:nodes test)
       (map (fn [node]
              (str node "=" (raft-addr node))))
       (str/join ",")))

(defn db
  "Rqlite for a particular version."
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing rqlite" version)
      (c/su
        (let [url (str "https://github.com/rqlite/rqlite/releases/download/" version 
                       "/rqlite-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))
        (cu/start-daemon!
          {:logfile logfile
           :pidfile pidfile
           :chdir dir}
          binary
          :-http-addr (http-addr node)
          :-raft-addr (raft-addr node)
          :-bootstrap-expect (count (:nodes test))
          :-join (initial-cluster test)))
      (Thread/sleep 1000))

    (teardown! [_ test node]
      (info node "tearing down rqlite")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :rm :-rf dir)))))

(defn rqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "rqlite"
          :os debian/os
          :db (db "v7.3.1")
           :pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn rqlite-test})
            args))