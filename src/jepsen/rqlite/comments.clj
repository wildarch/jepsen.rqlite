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
            [jepsen.rqlite.nemesis :as nem]
            [clojure.core.reducers :as r]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [knossos.model :as model]
            [knossos.op :as op])
  (:import com.rqlite.Rqlite)
  (:import com.rqlite.RqliteFactory
           (com.rqlite.dto QueryResults))
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

(defn query-results-value
  [results]
  (do (assert (instance? QueryResults results))
      (->> (.-results results)
           first
           .-values
           first
           first)))

(defrecord Client [table-count table-created? conn]
  jepsen.client/Client

  (open! [this test node]
    (assoc this :conn (RqliteFactory/connect "http" node (int 4001))))

  (setup! [this test]
    (Thread/sleep 2000)
    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (info "Creating tables")
        (doseq [t (table-names table-count)]
          (.Execute conn (str "create table " t
                              " (id int primary key,
                                           key int)"))
          (info "Created table")))))

    ;; A keyrange is used to track which keys a test is using, so we can split
    ;; them. This function takes a test and updates its :keyrange atom to include
    ;; the given table and key.
  (invoke! [this test op]
    (try
      (case (:f op)
        :write (let [[k id] (:value op)
                     table (id->table table-count id)]
                 (.Execute conn (str "INSERT INTO " table " VALUES ('" id "," k "')"))
                 (assoc op :type :ok))
        :read
        (->> (table-names table-count)
             (mapcat (fn [table]
                       (query-results-value
                        (.Query conn (str "SELECT id FROM "
                                          table
                                          " WHERE key = " (key (:value op))) com.rqlite.Rqlite$ReadConsistencyLevel/STRONG))))
             ;;(map :id)
             (into (sorted-set))
             (independent/tuple (key (:value op)))
             (assoc op :type :ok, :value)))

      (catch com.rqlite.NodeUnavailableException e
        (error "Node unavailable")
        (assoc op :type :fail , :error :not-found))

      (catch java.lang.NullPointerException e
        (error "Connection error")
        (assoc op
               :type  (if (= :read (:f op)) :fail :info)
               :error :connection-lost))))

  (teardown! [this test]
    nil)

  (close! [_ test]
    nil))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
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

(defn test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [reads (reads)
        writes (writes)]
    (merge rqlite/basic-test
           {:name "comments"
            :client (Client. 5 (atom false) nil)
            :nemesis (case (:nemesis-type opts)
                       :partition (nemesis/partition-random-halves)
                       :hammer (nemesis/hammer-time "rqlited")
                       :flaky (nem/flaky)
                       :slow (nem/slow 1.0))
            :generator  (->> (independent/concurrent-generator
                              (:concurrency opts)
                              (range)
                              (fn [k]
                                (->> (gen/mix [reads writes])
                                     (gen/stagger (/ (:rate opts)))
                                     (gen/limit (:ops-per-key opts)))))
                             (gen/nemesis
                              (cycle [(gen/sleep 5)
                                      {:type :info, :f :start}
                                      (gen/sleep 5)
                                      {:type :info, :f :stop}]))
                             (gen/time-limit 30))
            :checker (checker/compose
                      {:perf       (checker/perf)
                       :sequential (independent/checker (checker))})}
           opts)))