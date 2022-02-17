(ns jepsen.rqlite.client
  (:require [jepsen.client :as client]))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    this)

  (setup! [this test])

  (invoke! [_ test op])

  (teardown! [this test])

  (close! [_ test]))