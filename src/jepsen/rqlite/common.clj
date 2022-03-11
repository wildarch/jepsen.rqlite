(ns jepsen.rqlite.common
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian])
  (:import (com.rqlite.dto QueryResults)))

(def dir "/opt/rqlite")
(def binary "rqlited")
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
  \"foo:4002,bar:4002,...\""
  [test thisnode]
  (->> (:nodes test)
       (map (fn [node]
              (str "http://" (http-addr node))))
       (str/join ",")))

(defn data-dir
  "The directory to keep the raft log."
  [node]
  (str "/tmp/node-" node))

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
        :--http-addr (http-addr node)
        :--raft-addr (raft-addr node)
        :--bootstrap-expect (count (:nodes test))
        :--join (initial-cluster test node)
        (data-dir node)))
      (Thread/sleep 1000))

    (teardown! [_ test node]
      (info node "tearing down rqlite")
      (c/su
       (cu/stop-daemon! binary pidfile)
       (c/exec :rm :-rf dir)))
    db/LogFiles
    (log-files [_ test node]
      [logfile])))

(def basic-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  (merge tests/noop-test
         {:name "rqlite-basic-test"
          :os debian/os
          :db (db "v7.3.1")
          :pure-generators true}))