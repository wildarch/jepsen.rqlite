(ns jepsen.rqlite.comments
  "Checks for a strict serializability anomaly in which T1 < T2, but T2 is
    visible without T1.
    We perform concurrent blind inserts across n tables, and meanwhile, perform
    reads of both tables in a transaction. To verify, we replay the history,
    tracking the writes which were known to have completed before the invocation
    of any write w_i. If w_i is visible, and some w_j < w_i is *not* visible,
    we've found a violation of strict serializability.
    Splits keys up onto different tables to make sure they fall in different
    shard ranges"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen
             [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]
             [reconnect :as rc]
             [nemesis :as nemesis]]
            [jepsen.rqlite.common :as rqlite]

            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op])
  (:import com.rqlite.Rqlite)
  (:import com.rqlite.RqliteFactory)
  (:import (knossos.model Model)))

(def table-prefix "String prepended to all table names." "comment_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn id->table
  "Turns an id into a table id"
  [table-count id]
  (str table-prefix (mod (hash id) table-count)))

(defn reads [] {:type :invoke, :f :read, :value nil})

(defn writes []
  (->> (range)
       (map (fn [k] {:type :invoke, :f :write, :value k}))))

(defrecord Client [table-count table-created? client]
  jepsen.client/Client

  (open! [this test node]
    (assoc this :client (RqliteFactory/connect "http" node (int 4001))))

  (setup! [this test]
    (Thread/sleep 2000)
    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (info "Creating tables")
        (doseq [t (table-names table-count)]
          (.Execute client [(str "create table " t
                                 " (id int primary key,
                                           key int)")])
          (info "Created table")))))

    ;; A keyrange is used to track which keys a test is using, so we can split
    ;; them. This function takes a test and updates its :keyrange atom to include
    ;; the given table and key.
  (invoke! [this test op]
    (try
      (case (:f op)
        :write (let [[k id] (:value op)
                     table (id->table table-count id)]
                 (.Execute client (str "INSERT INTO " table " VALUES ('" k "')"))

                 (assoc op :type :ok))

        :read
        (->> (table-names table-count)
             (mapcat (fn [table]
                       (.Execute client [(str "select id from "
                                              table
                                              " where key = ?")
                                         (key (:value op))])))

             (map :id)
             (into (sorted-set))
             (independent/tuple (key (:value op)))
             (assoc op :type :ok, :value)))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (rc/close! client)))

(defn test
  [opts]
  (let [reads (reads)
        writes (writes)]
    (rqlite/basic-test
     (merge
      {:name   "comments"
       :client {:client (Client. 10 (atom false) nil)
                :during (independent/concurrent-generator
                         (count (:nodes opts))
                         (range)
                         (fn [k]
                           (->> (gen/mix [reads writes])
                                (gen/stagger 1/100)
                                (gen/limit 500))))
                :final  nil}}
      opts))))