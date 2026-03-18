# Live Recording & Offline Testing

## Overview

The tests in `ForceDriverConnectivityTest` can run in two modes:

-   **Live (recording)**: they connect to Salesforce, execute queries, and save the necessary data structures (fixtures) to files.
-   **Offline**: they load the saved fixtures and test the SQL→SOQL→ResultSet pipeline without a Salesforce connection.

## Fixture Structure

```
src/test/resources/fixtures/connectivity/
  <testName>/
    describe/
      Account.xml            # DescribeSObjectResult serialized with XStream
      Contact.xml
      ...
    query_result.xml          # List<List<ForceResultField>> serialized with XStream
```

## How to Record Fixtures

1.  Remove `@Disabled` from the desired `_record` test (e.g., `selectWithSubquery_record`).
2.  Run the test with a live connection to Salesforce.
3.  The fixtures are automatically saved in `src/test/resources/fixtures/connectivity/<testName>/`.
4.  Add `@Disabled` back to the test.

```bash
# Example: record fixtures for the "selectWithSubquery" test
mvn test -Dtest="ForceDriverConnectivityTest#selectWithSubquery_record"
```

## How to Run Offline Tests

The offline tests are in the `OfflineTests` inner class and run automatically with `mvn test`. If the fixtures do not exist yet, the tests are skipped with an explanatory message.

```bash
# Run all offline tests
mvn test -Dtest="ForceDriverConnectivityTest$OfflineTests"

# Run a single offline test
mvn test -Dtest="ForceDriverConnectivityTest$OfflineTests#selectWithEscapedQuoteInParameter"
```

## Helper Classes

| Class                                       | Description                                                                              |
| ------------------------------------------- | ---------------------------------------------------------------------------------------- |
| `TestFixtureUtils`                          | Utilities for XStream serialization/deserialization and fixture path management.         |
| `TestFixtureUtils.FileBackedPartnerService` | Extends `PartnerService`, loads `describeSObject()` and `query()` from fixture files.    |
| `TestFixtureUtils.RecordingPartnerService`  | Extends `PartnerService`, delegates to the real service, and saves the results to files. |

## Live Test → Offline Test Mapping

| Live Test (recording)                              | Offline Test                                        | What it tests                                  |
| -------------------------------------------------- | --------------------------------------------------- | ---------------------------------------------- |
| `selectWithSubquery_record`                        | `selectWithSubqueryReturnsExpectedRowCount`         | Cached SELECT with subquery, row count assert  |
| `selectWithParametersAndNestedSubquery_record`     | `selectWithParametersAndNestedSubquery`             | Parameterized query with nested subquery       |
| `selectWithTwoLevelRelation_record`                | `selectWithTwoLevelRelationResolvesFields`          | Two-level relation traversal (Order→Quote→Sub) |
| `selectWithMultiObjectRelation_record`             | `selectWithMultiObjectRelationReturnsCorrectValues` | Deep multi-object relation, value assertions   |
| `selectWithSelfPrefixedRelation_record`            | `selectWithSelfPrefixedRelationResolvesAllColumns`  | Self-prefixed relation (Order.Zuora_Quote__r)  |
| `selectWithNestedSubqueryColumnTypes_record`       | `selectWithNestedSubqueryResolvesCorrectColumnTypes`| Column type resolution on nested subquery      |
| `selectWithEscapedQuoteInParameter_record`         | `selectWithEscapedQuoteInParameter`                 | Escaped quote in parameter binding             |
| `selectWithAggregateMax_record`                    | `selectWithAggregateFunction`                       | Aggregate function (MAX)                       |

## Additional Live-Only Tests (no offline counterpart)

| Test                                       | Type     | Description                                 |
| ------------------------------------------ | -------- | ------------------------------------------- |
| `selectWithRelationTraversalOnBeam_record` | SELECT   | Beam relation traversal on Subscription_Beam|
| `selectStar_record`                        | SELECT   | SELECT * expansion                          |
| `selectWithLiterals_record`                | SELECT   | Literal values in SELECT                    |
| `selectWithEscapedQuoteInWhere_record`     | SELECT   | Escaped quote in WHERE clause               |
| `selectWithDateParameter_record`           | SELECT   | Date parameter binding                      |
| `selectWithTimestampFilter_record`         | SELECT   | Timestamp filter in WHERE                   |
| `updateContactFields`                      | DML      | Update multiple fields                      |
| `updateFieldsToNull`                       | DML      | Set fields to NULL                          |
| `updateWithPreparedStatement`              | DML      | Update with PreparedStatement               |
| `insertAndRetrieveGeneratedKey`            | DML      | Insert and get generated key                |
| `describeSObjectAndSaveXml_record`         | Metadata | Save DescribeSObjectResult to XML           |
| `loadAllTableMetadata_record`              | Metadata | Load metadata for all tables                |
