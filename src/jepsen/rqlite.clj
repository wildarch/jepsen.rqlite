(ns jepsen.rqlite
  (:require [jepsen [cli :as cli]]
            [jepsen.rqlite.register :as register]
            [jepsen.rqlite.sequential :as sequential]))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn register/test})
                   (cli/serve-cmd))
            args))