(ns jepsen.rqlite.register
  (:refer-clojure :exclude [test fake-test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [checker :as checker]
             [client :as client]
             [control :as c]
             [generator :as gen]
             [independent :as independent]
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

        ; Create a bunch of registers
        (doseq [i (range 100)]
          (.Execute conn (str "insert into test values (" i ", 0)"))))))

  (invoke! [this test op]
    (let [[k v] (:value op)]
      (try
        (case (:f op)
          :read (let [results
                      (.Query conn
                              (str "SELECT val from test where id = " k)
                              com.rqlite.Rqlite$ReadConsistencyLevel/STRONG)]
                  (assoc op :type :ok, :value (independent/tuple k (query-value results))))

          :write (let [results (.Execute conn (str
                                               "update test set val = "
                                               v
                                               " WHERE id = "
                                               k))]
                   (assoc op :type (if
                                    (== 1 (.-rowsAffected (first (.-results results))))
                                     :ok
                                     :fail)))

          :cas (let [[old new] v]
                 (let [results (.Execute conn (str
                                               "update test set val = "
                                               new
                                               " WHERE id = "
                                               k
                                               " AND val = "
                                               old))]
                   (assoc op :type (if
                                    (== 1 (.-rowsAffected (first (.-results results))))
                                     :ok
                                     :fail)))))

        (catch com.rqlite.NodeUnavailableException e
          (error "Node unavailable")
          (assoc op :type :fail , :error :not-found))

        (catch java.lang.NullPointerException e
          (error "Connection error")
          (assoc op
                 :type  (if (= :read (:f op)) :fail :info)
                 :error :connection-lost)))))

  (teardown! [this test])

  (close! [_ test]))

(defn test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge rqlite/basic-test
         {:name "register"
          :client (Client. (atom false) nil)
          :nemesis (case (:nemesis-type opts)
                     :partition (nemesis/partition-random-halves)
                     :hammer (nemesis/hammer-time "rqlited"))
          :checker (checker/compose
                    {:perf   (checker/perf)
                     :indep (independent/checker
                             (checker/compose

                              {:linear (checker/linearizable
                                        {:model     (model/cas-register 0)
                                         :algorithm :linear})

                               :timeline (timeline/html)}))})
          :generator  (->> (independent/concurrent-generator
                            (:concurrency opts)
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w cas])
                                   (gen/stagger (/ (:rate opts)))
                                   (gen/limit (:ops-per-key opts)))))
                           (gen/nemesis
                            (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 5)
                                    {:type :info, :f :stop}]))
                           (gen/time-limit (:time-limit opts)))}
         opts))