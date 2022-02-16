(ns jepsen.rqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [org.httpkit.client :as http])
  (:import com.rqlite.Rqlite)
  (:import com.rqlite.RqliteFactory))

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

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn query-results-string
  "Stringifies query results"
  [results]
  (map (fn [res] (.toPrettyString res)) (seq (.results results))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (RqliteFactory/connect "http" node (int 4001))))

  (setup! [this test])

  (invoke! [this test op]
    (case (:f op)
      :read (assoc op :type :ok, :value (query-results-string (.Query conn "SELECT 1" com.rqlite.Rqlite$ReadConsistencyLevel/STRONG)))))

  (teardown! [this test])

  (close! [_ test]))

(defn rqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         {:name "rqlite"
          :os debian/os
          :db (db "v7.3.1")
          :client (Client. nil)
          :generator       (->> r
                                (gen/stagger 1)
                                (gen/nemesis nil)
                                (gen/time-limit 15))
          :pure-generators true}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn rqlite-test})
            args))