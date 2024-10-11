# Changelog

### 1.5.5

#### Features

* Updated jsqlparser to custom version for includes/excludes: Better control over parsing to streamline query execution.

#### Fixes & Improvements

* Fixed subquery relationship check: Ensured accurate relationship checks for subqueries, improving query accuracy.
* Removed MapDB in favor of EHCache: Streamlined caching mechanism for better performance and reduced dependencies tree.

#### Technical Updates

* Upgraded to Java 17
* Upgraded to junit 5: Enhancing testing capabilities for better code quality assurance.
* Refactored query analyzer and processor: Streamlined query processing, optimized caching, and removed deprecated features for improved performance.
* Added tree as query result: Now, queries return tree structures for enhanced data visualization and analysis.


### 1.4.4
SOQL queries will try to expand the `SELECT * from Account` syntax for root query entity into up to 100 fields.

### 1.4.3
Parsing of Date Value into SOQL date for IntelliJ.

Example: {ts '2021-10-21 12:01:02Z'}

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