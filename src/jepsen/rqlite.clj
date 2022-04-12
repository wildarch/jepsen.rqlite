(ns jepsen.rqlite
  (:require [jepsen [cli :as cli]]
            [jepsen.rqlite.register :as register]
            [jepsen.rqlite.comments :as comments]
            [jepsen.rqlite.sequential :as sequential]))

(def cli-opts
  "Additional command line options."
  [[nil "--read-consistency none|weak|strong" "Set consistency level for reads."
    :default :strong
    :parse-fn #(case %
                 ("none") :none
                 ("weak") :weak
                 ("strong") :strong)
    :validate [#{:none :weak :strong} "Unsupported read consistency"]]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  50
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  1000
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be a positive integer."]]
   [nil "--nemesis-type partition|hammer|flaky|slow|noop" "Nemesis used."
    :default :partition
    :parse-fn #(case %
                 ("partition") :partition
                 ("hammer") :hammer
                 ("flaky") :flaky
                 ("slow") :slow
                ("noop") :noop
                 :invalid)
    :validate [#{:partition :hammer :flaky :slow :noop} "Unsupported nemesis"]]])

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/test-all-cmd {:tests-fn (fn [opts] [(register/test opts)
                                                            (comments/test opts)
                                                            (sequential/test opts)])
                                      :opt-spec cli-opts})
                   (cli/serve-cmd))
            args))