(defproject explojure "0.1.0-SNAPSHOT"
  :description "A simple, solid library for data munging in Clojure."
  :url "http://github.com/aaronj1331/explojure"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]]
  :plugins [[cider/cider-nrepl "0.8.2"]]
  :repositories [["local" {:url "file:///analysis/clj/repo"
                           :sign-releases false}]]
  :deploy-repositories [["local" {:url "file:///analysis/clj/repo"
                                  :sign-releases false}]])
