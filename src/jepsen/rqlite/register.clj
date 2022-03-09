(ns jepsen.rqlite.register
  (:refer-clojure :exclude [test fake-test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [client :as client]
             [control :as c]
             [generator :as gen]
             [nemesis :as nemesis]]
            [jepsen.rqlite.common :as rqlite]
            [jepsen.checker.timeline :as timeline]
            [knossos.model :as model])
  (:import com.rqlite.Rqlite)
  (:import com.rqlite.RqliteFactory)
  (:import (knossos.model Model)))

(defn r   [_ _] {:type :invoke, :f :read, :value 0})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn query-value
  "Extracts the value returned from the query"
  [results]
  ; Coerce to integer
  (int
    ; First value
   (first
      ; First row
    (first
     (.-values (first (.-results results)))))))

(defrecord Client [tbl-created? conn]
  client/Client
  (open! [this test node]
    (Thread/sleep 3000)
    (assoc this :conn (RqliteFactory/connect "http" node (int 4001))))

  (setup! [this test]
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        (Thread/sleep 1000)
        (.Execute conn "drop table if exists test")
        (Thread/sleep 1000)
        (.Execute conn "create table test (id int primary key, val int)")
        (.Execute conn "insert into test values (1, 0)"))))

  (invoke! [this test op]
    (try
      (case (:f op)
        :read (let [results
                    (.Query conn "SELECT val from test where id = 1" com.rqlite.Rqlite$ReadConsistencyLevel/STRONG)]
                (try
                  (assoc op :type :ok, :value (query-value results))
                  (catch java.lang.NullPointerException e
                    (error "Failed to read query value")
                    (assoc op :type :fail))))

        :write (do
                 (.Execute conn (str
                                 "update test set val = "
                                 (:value op)
                                 " WHERE id = 1"))

                 (assoc op :type :ok))
        :cas (let [[old new] (:value op)]
               (let [results (.Execute conn (str
                                             "update test set val = "
                                             new
                                             " WHERE id = 1 AND val = "
                                             old))]

                 (try
                   (assoc op :type (if
                                    (== 1 (.-rowsAffected (first (.-results results))))
                                     :ok
                                     :fail))

                   (catch java.lang.NullPointerException e
                     (error "Failed to get number of changed rows")
                     (assoc op :type :info :error :no-results))))))

      (catch com.rqlite.NodeUnavailableException e
        (assoc op :type :fail , :error :not-found))))

  (teardown! [this test])

  (close! [_ test]))

(defn test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge rqlite/basic-test
         {:name "register"
          :client (Client. (atom false) nil)
          :nemesis         (nemesis/partition-random-halves)
          :checker (checker/compose
                    {:perf   (checker/perf)
                     :linear (checker/linearizable
                              {:model     (model/cas-register 0)
                               :algorithm :linear})
                     :timeline (timeline/html)})
          :generator       (->> (gen/mix [r w cas])
                                (gen/stagger 1/50)
                                (gen/nemesis
                                 (cycle [(gen/sleep 5)
                                         {:type :info, :f :start}
                                         (gen/sleep 5)
                                         {:type :info, :f :stop}]))
                                (gen/time-limit 30))}
         opts))