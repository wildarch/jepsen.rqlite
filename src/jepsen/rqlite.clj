(ns jepsen.rqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.control.docker :as docker]
            [jepsen.os.debian :as debian]))

(def dir "/opt/rqlite")
(def binary "rqlited")
(def logfile (str dir "/rqlite.log"))
(def pidfile (str dir "/rqlite.pid"))

(defn client-addr [node]
  (str node ":4001"))
(defn peer-addr [node]
  (str node ":4002"))

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
          {:logfile logfile
           :pidfile pidfile
           :chdir   dir}
          binary
          :-node-id     (name node)
          :-http-addr   (client-addr node)
          :-raft-addr   (peer-addr node)))
      (Thread/sleep 10000))

    (teardown! [_ test node]
      (info node "tearing down rqlite")
      (cu/stop-daemon! binary pidfile)
      (c/su (c/exec :echo :rm :-rf dir)))))


(defn rqlite-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name            "rqlite"
          :os              debian/os
          :db              (db "v7.3.1")
          :remote          docker/docker
          :pure-generators true
          :sudo            false}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn rqlite-test})
                   (cli/serve-cmd))
            args))
