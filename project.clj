(defproject explojure "0.7.0"
  :description "A simple, solid library for data munging in Clojure."
  :url "http://github.com/aaronj1331/explojure"
  :lein-release {:scm :git
                 :deploy-via :shell
                 :shell ["lein" "deploy" "local"]}
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.apache.commons/commons-csv "1.2"]
                 [org.apache.commons/commons-math3 "3.6.1"]])

