(ns jepsen.rqlite
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.client :as client]
            [clj-http.client :as http]
            [jepsen.generator :as gen]
            [slingshot.slingshot :refer [try+]]))

(def dir "/opt/rqlite")
(def binary "rqlited")
(def logfile (str "/opt/rqlite.log"))
(def pidfile (str dir "/rqlite.pid"))

(defn client-addr [node]
  (str node ":4001"))
(defn execute-uri [node]
  (str "http://" (client-addr node) "/db/execute"))
(defn query-uri [node]
  (str "http://" (client-addr node) "/db/query"))
(defn peer-addr [node]
  (str node ":4002"))
(defn initial-cluster [test this]
  (->> (:nodes test)
       (map (fn [node]
                (str "http://" (client-addr node))))
       (str/join ",")))

(defn r [_ _] {:type :invoke, :f :read, :value nil})
(defn i [_ _] {:type :invoke, :f :insert, :value (rand-int 5)})
(defn u [_ _] {:type :invoke, :f :update, :value (rand-int 5)})
(defn d [_ _] {:type :invoke, :f :delete, :value nil})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})


(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn node))

  (setup! [this test]
    (info "Creating db" (execute-uri conn))
    (http/post (execute-uri conn)
               {:content-type :json
                :body         "[\"CREATE TABLE foo (key VARCHAR(10), value INTEGER)\"]"}))

  (invoke! [this test op]
    (case (:f op)
      :read (do (->> (http/get (query-uri conn)
                               {:query-params {:q "SELECT value FROM foo WHERE key = 'foo' LIMIT 1"}
                                :accept       :json})
                     :body :results first :values first first
                     (assoc op :value))
                (assoc op :type (if (:value op) :ok :fail)))
      :insert (http/post (execute-uri conn)
                         {:content-type :json
                          :body         (str "[\"INSERT INTO foo VALUES ('foo', " (:value op) ")\"]")})
      :update (http/post (execute-uri conn)
                         {:content-type :json
                          :body         (str "[\"UPDATE foo SET value = " (:value op) " WHERE key = 'foo'\"]")})
      :delete (http/post (execute-uri conn)
                         {:content-type :json
                          :body         (str "[\"DELETE FROM foo WHERE key = 'foo'\"]")})
      :cas nil))

  (teardown! [this test]
    (http/post (execute-uri conn)
               {:content-type :json
                :body         "[\"DROP TABLE foo\"]"}))

  (close! [_ test]))

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
        (info node "joining" (initial-cluster test node))
        (cu/start-daemon!
          {:logfile       logfile
           :pidfile       pidfile
           :make-pidfile? true
           :chdir         dir}
          binary
          :-node-id (name node)
          :-http-addr (client-addr node)
          :-raft-addr (peer-addr node)
          :-bootstrap-expect (count (:nodes test))
          :-join (initial-cluster test node)
          "~/node_data"))
      (Thread/sleep 10000))

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
         {:name      "rqlite"
          :os        ubuntu/os
          :db        (db "v7.3.1")
          :client    (Client. nil)
          :ssh       {:username                 "rqlite"
                      :password                 "rqlite"
                      :strict-host-key-checking false
                      :port                     22
                      :sudo-password            "rqlite"}

          :generator (->> r
                          (gen/stagger 1)
                          (gen/nemesis nil)
                          (gen/time-limit 15))}))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn rqlite-test})
                   (cli/serve-cmd))
            args))
