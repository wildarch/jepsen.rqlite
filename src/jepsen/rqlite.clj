(ns jepsen.rqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.rqlite.client as client])
  (:import (jepsen.rqlite.client Client)))

(def dir "/opt/rqlite")
(def binary "rqlited")
(def logfile (str dir "/rqlite.log"))
(def pidfile (str dir "/rqlite.pid"))

(defn client-addr [node]
  (str node ":4001"))
(defn peer-addr [node]
  (str node ":4002"))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn w [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn db
  "rqlite DB for a particular version"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (info node "installing rqlite" version)
      (c/su
        (let [url (str "https://github.com/rqlite/rqlite/releases/download/" version
                       "/rqlite-" version "-linux-amd64.tar.gz")]
          (cu/install-archive! url dir))
        (cu/start-daemon!
          {:logfile       logfile
           :pidfile       pidfile
           :make-pidfile? true
           :chdir         dir}
          binary
          :-node-id (name node)
          :-http-addr (client-addr node)
          :-raft-addr (peer-addr node)
          "~/node_data"))
      (Thread/sleep 30000))

    (teardown! [_ test node]
      (info node "tearing down rqlite")
      (c/su (cu/stop-daemon! binary pidfile)
            (c/exec :rm :-rf dir)
            (c/exec :rm :-rf "~/node_data")))))


(defn rqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name            "rqlite"
          :os              ubuntu/os
          :db              (db "v7.3.1")
          :client          (Client. nil)
          :ssh             {:username                 "rqlite"
                            :password                 "rqlite"
                            :strict-host-key-checking false
                            :port                     22
                            :sudo-password            "rqlite"
                            }
          :pure-generators true}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn rqlite-test})
                   (cli/serve-cmd))
            args))
