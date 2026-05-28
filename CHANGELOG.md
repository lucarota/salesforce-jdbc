# Changelog

### 2.0.2

- **Standard Nullity Expression Rewriting**:
  - Added rewriting support for standard `IS NULL` and `IS NOT NULL` expressions in SOQL queries, translating them to `= NULL` and `<> NULL` to comply with Salesforce SOQL syntax constraints.
- **Support for Client-Side COALESCE Function**:
  - Implemented the client-side `COALESCE` function in both `SELECT` projections and `WHERE` clauses.
  - In `SELECT` projections, it automatically rewrites incoming queries to request all underlying fields, resolving the first non-null value in-memory.
  - In `WHERE` clauses, it recursively rewrites comparisons containing `COALESCE` into standard SOQL boolean logic (using `AND`/`OR` chains) so filtering runs database-side, performing client-side constant folding to simplify logic and prune false literal comparison branches.
- **Dependency Refactoring**:
  - Migrated from the deprecated `Parenthesis` class to `ParenthesedExpressionList` in JSQLParser.
- **Documentation**:
  - Added details on custom functions, performance implications, and `WHERE` clause behavior to the README.

### 2.0.1

- **Support for OAuth 2.0 Client Credentials Authentication**:
  - Implemented client credentials flow using `clientId` and `clientSecret` parameters.
  - Automatically fetches OAuth 2.0 access tokens and resolves organization metadata without modifying the core `force-partner-api` library.
  - Handles automatic session renewal and retry on expired/invalidated tokens.
  - Added caching for user info and access tokens to avoid redundant token requests.
- **Enhanced Configuration & Validation**:
  - Requires a custom `loginDomain` for Client Credentials authentication, raising an error for unsupported generic domains (`login.salesforce.com` or `test.salesforce.com`).
  - Added warning logs when using the legacy `sandbox` parameter along with OAuth parameters.
- **Security & Logging Improvements**:
  - Added masking of `clientSecret` / `client_secret` parameter values in JDBC URLs, connection info logs, and error messages.
- **Build Infrastructure & Tooling**:
  - Upgraded Project Lombok to `1.18.36` for complete compatibility with JDK 21+ and JDK 25.
  - Configured explicit annotation processor paths in Maven compiler plugin.
- **Testing Improvements**:
  - Added unit tests for OAuth token handling and error retries.
  - Added pre-production integration tests (`selectOAuthPreprod_record`, `insertOAuthPreprod_record`, `updateOAuthPreprod_record`) in `ForceDriverConnectivityTest` using live Preprod credentials.

### 2.0.0

- **Preserved SQL SELECT field order in flattened results**:
  - Added tracking of fields in their original SQL `SELECT` order.
  - Flattened field definitions now preserve the order written in the query, even when relationship fields are internally reorganized for Salesforce API compatibility.
  - Untracked fields such as subqueries or computed fields are still appended correctly.

- **Improved result expansion for missing relationship values**:
  - Expanded result rows now handle missing relationship fields more safely.
  - When Salesforce omits relation values, placeholder fields are inserted to keep the result structure aligned with the schema.

- **Introduced a shared abstract base for prepared statements**:
  - Added a new `AbstractPreparedStatement` class to centralize default stub implementations for unsupported JDBC `PreparedStatement` methods.
  - This reduces duplication and makes statement implementations easier to maintain.

- **Connection handling improvements**:
  - Improved Salesforce connection lifecycle structure with clearer support for refreshed partner connections after reconnect or re-login operations.
  - Added a unique internal identifier for each connection instance to better support session-scoped resources such as caches.

- **Documentation and maintainability improvements**:
  - Expanded inline documentation for connection and statement behavior.
  - Clarified which JDBC features are intentionally unsupported or implemented as no-ops in the Salesforce driver.

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