# Changelog

### 1.6.6
 
- **Support for JDBC date/timestamp literals**:
  - Added pre-processing for SOQL queries to convert the JDBC escape syntax `'{ts ...}'`
    into a SOQL-compatible date/time format.
  - Parameter conversion now also correctly handles date strings that follow this format.

- **Code Modernization and Refactoring**:
  - Replaced imperative code with more modern and concise Stream APIs, particularly in the
    `flatten` and `next` methods.
  - Improved null handling using `Optional.ofNullable`.
  - Refactored the `findColumnType` method for a cleaner column type lookup.

- **Improved State Management**:
  - The `executeInternal` method now more comprehensively resets the internal state of the
    statement (e.g., `metadata`, `resultSet`, query analyzers) before each execution, preventing side effects between calls.

- **More Robust Subquery Resolution**:
  - The `runResolveSubselect` method has been improved by using a `try-with-resources` block
    to ensure the proper closing of the `ResultSet`.

- **Correct Handling of Query After `*` Expansion**:
  - The internal SOQL query is now correctly updated after `SoqlQueryAnalyzer` expands the `SELECT *`
    syntax into the actual fields.

#### Features

* Updated jsqlparser to version 5.1 for includes/excludes: Better control over parsing to streamline query execution.

#### Fixes & Improvements

* Fixed subquery relationship check: Ensured accurate relationship checks for subqueries, improving query accuracy.
* Removed MapDB in favor of EHCache: Streamlined caching mechanism for better performance and reduced dependencies tree.

#### Technical Updates

* Upgraded to Java 21
* Upgraded to junit 5: Enhancing testing capabilities for better code quality assurance.
* Refactored query analyzer and processor: Streamlined query processing, optimized caching, and removed deprecated features for improved performance.
* Added tree as query result: Now, queries return tree structures for enhanced data visualization and analysis.


### 1.4.4
SOQL queries will try to expand the `SELECT * from Account` syntax for root query entity into up to 100 fields.

### 1.4.3
Parsing of Date Value into SOQL date for IntelliJ.

Example: '{ts 2021-10-21 12:01:02Z}'

### 1.4.2
SELECT of child relationship command parsing

### 1.4.1
DELETE command parsing

### 1.4.0
INSERT/UPDATE command parsing

### 1.3.1.3
CONNECT command parsing fixes

### 1.3.1.0
Re-connection to a different host using CONNECT command

### 1.3.0.1
   Insecure HTTPS - disabling the SSL Certificate verification

### 1.2.6.03
   Update force-partner-api to 51.0.0

### 1.2.6.02
   Fields for SubSelects aliases

   Returning flat resultset for field

### 1.2.6.01
   Update force-partner-api to 50.0.0

   Implement parameters:
* loginDomain
* client
* https
* api

   Implement missing JDBC methods which are required for JetBrains IntelliJ IDE