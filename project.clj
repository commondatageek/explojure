(defproject explojure "0.4.0"
  :description "A simple, solid library for data munging in Clojure."
  :url "http://github.com/aaronj1331/explojure"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.csv "0.1.2"]]
  :plugins [[cider/cider-nrepl "0.9.1"]]
  :repositories [["local" {:url "file:///analysis/clj/repo"
                           :sign-releases false}]]
  :deploy-repositories [["local" {:url "file:///analysis/clj/repo"
                                  :sign-releases false}]])
