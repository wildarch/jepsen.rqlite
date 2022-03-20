(ns jepsen.rqlite
  (:require [jepsen [cli :as cli]]
            [jepsen.rqlite.register :as register]))

(def cli-opts
  "Additional command line options."
  [["-q" "--quorum" "Use quorum reads, instead of reading from a likely leader node."
    :default true]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  50
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive integer."]]
   [nil "--nemesis-type partition|hammer" "Nemesis used."
    :default :partition
    :parse-fn #(case %
                 ("partition") :partition
                 ("hammer") :hammer
                 :invalid)
    :validate [#{:partition :hammer} "Unsupported nemesis"]]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn register/test
                                         :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))