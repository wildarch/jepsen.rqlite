(ns jepsen.rqlite.sequential
  "A sequential consistency test.

  Verify that client order is consistent with DB order by performing queries
  (in four distinct transactions) like
  A: insert x
  A: insert y
  B: read y
  B: read x

  A's process order enforces that x must be visible before y, so we should
  always read both or neither."
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [jepsen.checker :as checker]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [clojure.core.reducers :as r]
            [knossos.op :as op]
            [jepsen.rqlite.common :as rqlite]
            [jepsen.rqlite.nemesis :as nem]
            [jepsen.cli :as cli])
  (:import com.rqlite.Rqlite)
  (:import com.rqlite.RqliteFactory
           (com.rqlite.dto QueryResults)
           (com.rqlite Rqlite$ReadConsistencyLevel)
           (jepsen.generator Generator)
           (clojure.lang PersistentQueue)))

; Code adapted from the sequential test for CockroachDB from:
; https://github.com/jepsen-io/jepsen/blob/main/cockroachdb/src/jepsen/cockroach/sequential.clj

(def table-prefix "String prepended to all table names." "seq_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn key->table
  "Turns a key into a table id"
  [table-count k]
  (str table-prefix (mod (hash k) table-count)))

(defn subkeys
  "The subkeys used for a given key, in order."
  [key-count k]
  (mapv (partial str k "_") (range key-count)))

(defn query-results-value
  "Returns the only value returned from the query"
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
    (Thread/sleep 3000)
    (assoc this :conn (RqliteFactory/connect "http" node (int 4001))))

  (setup! [this test]
    (locking table-created?
      (when (compare-and-set! table-created? false true)
        (info "Creating tables")
        (doseq [t (table-names table-count)]
          (.Execute conn (str "DROP TABLE IF EXISTS " t))
          (.Execute conn (str "CREATE TABLE " t " (key varchar(255) primary key)"))))))

  (invoke! [this test op]
    (let [ks (subkeys (:key-count test) (:value op))]
      (try
        (case (:f op)
          :write (do (doseq [k ks]
                       (let [table (key->table table-count k)]
                         (.Execute conn (str "INSERT INTO " table " VALUES ('" k "')"))))
                     (assoc op :type :ok))
          :read (->> ks
                     reverse
                     (mapv (fn [k]
                             (query-results-value
                              (.Query conn
                                      (str "SELECT key FROM "
                                           (key->table table-count k)
                                           " WHERE key = '" k "'")
                                      (case (:read-consistency test)
                                        :none com.rqlite.Rqlite$ReadConsistencyLevel/NONE
                                        :weak com.rqlite.Rqlite$ReadConsistencyLevel/WEAK
                                        :strong com.rqlite.Rqlite$ReadConsistencyLevel/STRONG)))))

                     (vector (:value op))
                     (assoc op :type :ok, :value)))
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

(defn writes
  "We emit sequential integer keys for writes, logging the most recent n keys
  in the given atom, wrapping a PersistentQueue."
  [last-written]
  (let [k (atom -1)]
    (fn []
      (let [k (swap! k inc)]
        (swap! last-written #(-> % pop (conj k)))
        {:type :invoke, :f :write, :value k}))))

(defn reads
  "We use the last-written atom to perform a read of a randomly selected
  recently written value."
  [last-written]
  (gen/filter (comp complement nil? :value)
              (fn [] {:type :invoke, :f :read, :value (rand-nth @last-written)})))

(defn gen
  "Basic generator with n writers, and a buffer of 2n"
  [n]
  (let [last-written (atom
                      (reduce conj PersistentQueue/EMPTY
                              (repeat (* 2 n) nil)))]
    (gen/reserve n (writes last-written)
                 (reads last-written))))

(defn trailing-nil?
  "Does the given sequence contain a nil anywhere after a non-nil element?"
  [coll]
  (some nil? (drop-while nil? coll)))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (assert (integer? (:key-count test)))
      (let [reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value)
                       (into []))
            none (filter (comp (partial every? nil?) second) reads)
            some (filter (comp (partial some nil?) second) reads)
            bad (filter (comp trailing-nil? second) reads)
            all (filter (fn [[k ks]]
                          (= (subkeys (:key-count test) k)
                             (reverse ks)))
                        reads)]
        {:valid?     (not (seq bad))
         :all-count  (count all)
         :some-count (count some)
         :none-count (count none)
         :bad-count  (count bad)
         :bad        bad}))))

(defn test
  [opts]
  (let [gen (gen 4)
        keyrange (atom {})]
    (merge rqlite/basic-test
           {:name      "sequential"
            :key-count 5
            :keyrange  keyrange
            :client    (Client. 10 (atom false) nil)
            :nemesis (case (:nemesis-type opts)
                      :partition (nemesis/partition-random-halves)
                      :hammer (nemesis/hammer-time "rqlited")
                      :flaky (nem/flaky)
                      :slow (nem/slow 1.0)
                      :noop nemesis/noop)
            :generator (->>
                        (gen/stagger 1/100 gen)
                        (gen/nemesis
                         (cycle [(gen/sleep 5)
                                 {:type :info, :f :start}
                                 (gen/sleep 5)
                                 {:type :info, :f :stop}]))
                        (gen/time-limit (:time-limit opts)))
            :checker   (checker)
            :read-consistency (:read-consistency opts)}
           opts)))