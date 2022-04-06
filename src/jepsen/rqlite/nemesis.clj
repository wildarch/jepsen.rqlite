(ns jepsen.rqlite.nemesis
  "Custom nemeses for testing rqlited"
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [jepsen
             [net :as net]
             [control :as c]
             [nemesis :as nemesis]]))

(defn heal
  "Heals the network, removing any slowdown or flaky delivery"
  [net test]
  (c/with-test-nodes test
    (try
      (c/su (c/exec "/sbin/tc" :qdisc :del :dev :eth1 :root))
      (catch RuntimeException e
        (if (re-find #"Cannot delete qdisc with handle of zero"
                     (.getMessage e))
          nil
          (throw e))))))

(defn set-delay [net test {:keys [mean variance distribution]
                           :or   {mean         50
                                  variance     10
                                  distribution :normal}}]
  (c/with-test-nodes test
    (c/su (c/exec "/sbin/tc" :qdisc :add :dev :eth1 :root :netem :delay
                  (str mean "ms")
                  (str variance "ms")
                  :distribution distribution))))

(defn slow
  "Slows the network by dt s on start. 
   Restores network speeds on stop."
  [dt]
  (reify nemesis/Nemesis
    (setup! [this test]
      (heal (:net test) test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (do (set-delay (:net test) test {:mean (* dt 1000) :variance 1})
                   (assoc op :type :info :value :started))

        :stop (do (heal (:net test) test)
                  (assoc op :type :info :value :stopped))))

    (teardown! [this test]
      (heal (:net test) test))))

(defn flaky
  "Introduces randomized packet loss on start. 
   Restores network integrity on stop."
  []
  (reify nemesis/Nemesis
    (setup! [this test]
      (heal (:net test) test)
      this)

    (invoke! [this test op]
      (case (:f op)
        :start (do
                 (c/with-test-nodes test
                   (c/su (c/exec "/sbin/tc" :qdisc :replace :dev :eth1 :root :netem :loss "20%"
                                 "75%")))
                 (assoc op :type :info :value :started))

        :stop (do (heal (:net test) test)
                  (assoc op :type :info :value :stopped))))

    (teardown! [this test]
      (heal (:net test) test))))