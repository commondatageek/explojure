(ns explojure.core)

(defprotocol Tabular
  "
  The Tabular protocol represents tabular data.  (Rows and columns).
  "
  ($col [this col]
    "Return the specified column(s)")
  ($row [this row]
    "Return the specified row(s)")
  ($ [this cols rows]
    "Select the specified columns and rows from this")
  ($nrow [this]
    "Return the number of rows")
  ($ncol [this]
    "Return the number of columns")
  ($colnames [this]
    "Return the column names")
  ($map [this src-col f] [this src-col f dst-col]
    "Return the result of (map)-ing src-col by function f.  If dst-col is
  given, return this Tabular with a new column.")
  ($count [this]
    "Show the number of non-nil values in each column")
  ($describe [this]))
