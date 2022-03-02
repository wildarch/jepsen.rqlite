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
  (:require [jepsen
                    [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [independent :as independent]
                    [util :as util :refer [meh]]
                    [reconnect :as rc]
                    [nemesis :as nemesis]]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op]))
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

(defrecord Client [table-count table-created? client]
  client/Client

  (open! [this test node]
    (assoc this :client (RqliteFactory/connect "http" node (int 4001)))

  (setup! [this test]
    (Thread/sleep 2000)
    (locking table-created?
      (when (compare-and-set! table-created? false true)
          (info "Creating tables" (pr-str (table-names table-count)))
          (doseq [t (table-names table-count)]
          (.Execute client [(str "create table " t
                                  " (id int primary key,
                                       key int)")])
          (info "Created table" )))))

  (invoke! [this test op]
    (try
            (case (:f op)
              :write (let [[k id] (:value op)
                           table (id->table table-count id)]
                       (c/insert! c table {:id id, :key k})
                       (cockroach/update-keyrange! test table id)
                       (assoc op :type :ok))

              :read (c/with-txn [c c]
                      (->> (table-names table-count)
                           (mapcat (fn [table]
                                     (c/query c [(str "select id from "
                                                      table
                                                      " where key = ?")
                                                 (key (:value op))])))
                           (map :id)
                           (into (sorted-set))
                           (independent/tuple (key (:value op)))
                           (assoc op :type :ok, :value))))))

  (teardown! [this test]
    nil)

  (close! [this test]
    (rc/close! client)))

(defn checker
  []
  (reify checker/Checker
    (check [this test model history opts]
      ; Determine first-order write precedence graph
      (let [expected (loop [completed  (sorted-set)
                            expected   {}
                            [op & more :as history] history]
                       (cond
                         ; Done
                         (not (seq history))
                         expected

                         ; We know this value is definitely written
                         (= :write (:f op))
                         (cond ; Write is beginning; record precedence
                               (op/invoke? op)
                               (recur completed
                                      (assoc expected (:value op) completed)
                                      more)

                               ; Write is completing; we can now expect to see
                               ; it
                               (op/ok? op)
                               (recur (conj completed (:value op))
                                      expected more)

                               true
                               (recur completed expected more))

                         true
                         (recur completed expected more)))
            errors (->> history
                        (r/filter op/ok?)
                        (r/filter #(= :read (:f %)))
                        (reduce (fn [errors op]
                                  (let [seen         (:value op)
                                        our-expected (->> seen
                                                          (map expected)
                                                          (reduce set/union))
                                        missing (set/difference our-expected
                                                                seen)]
                                    (if (empty? missing)
                                      errors
                                      (conj errors
                                            (-> op
                                                (dissoc :value)
                                                (assoc :missing missing)
                                                (assoc :expected-count
                                                       (count our-expected)))))))
                                []))]
        {:valid? (empty? errors)
         :errors errors}))))

(defn reads [] {:type :invoke, :f :read, :value nil})
(defn writes []
  (->> (range)
       (map (fn [k] {:type :invoke, :f :write, :value k}))
       gen/seq))

(defn test
  [opts]
  (let [reads (reads)
        writes (writes)]
  (merge rqlite/basic-test
        {:name   "comments"
         :client {:client (Client. 10 (atom false) nil)
         :generator    (->> (gen/mix [reads writes])
                       (gen/stagger 1/100)
                       (gen/limit 500))))
                  :final  nil}
         :checker (checker/compose
                    {:perf       (checker/perf)
                     :sequential (independent/checker (checker))})}
        opts))))