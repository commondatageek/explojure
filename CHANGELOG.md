# Change Log
All notable changes to this project will be documented in this file.
This change log follows the conventions of
[keepachangelog.com](http://keepachangelog.com/).

This project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Added
- explojuredataframe.core
    - (row-maps) allows to process rows as maps instead of vectors
    - (map-col) map a column to a function to create a new column
- explojure.util
    - (where) now accepts a function for transforming the coll.

### Fixed
- User was actually unable to specify :headers.  Now they should be able.

## [0.7.0] - 2016-04-19
### Added
- User is able to specify whether there is a header row on read-csv
