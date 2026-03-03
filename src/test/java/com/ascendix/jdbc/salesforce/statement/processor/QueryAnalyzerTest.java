package com.ascendix.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class QueryAnalyzerTest {

    @Nested
    @DisplayName("analyse() method tests")
    class AnalyseMethodTests {

        @Test
        @DisplayName("Should detect INSERT query")
        void testIsInsertQuery() {
            String soql = "insert into Account(Name, OwnerId) values ('FirstAccount', '005xx1231231233123')";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.INSERT));
        }

        @Test
        @DisplayName("Should detect UPDATE query")
        void testIsUpdateQuery() {
            String soql = "Update Account set Name ='FirstAccount_new' where Id='001xx000003GeY0AAK'";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.UPDATE));
        }

        @Test
        @DisplayName("Should detect SELECT query")
        void testIsSelectQuery() {
            String soql = "Select Id from Account";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }

        @Test
        @DisplayName("Should detect DELETE query")
        void testIsDeleteQuery() {
            String soql = "Delete from Account where id='001xx000003GeY0AAK'";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.DELETE));
        }

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = {"   ", "\t", "\n", "  \t  \n  "})
        @DisplayName("Should return false for null, empty, or whitespace-only queries")
        void testNullEmptyWhitespaceQueries(String soql) {
            QueryAnalyzer analyzer = new QueryAnalyzer("SELECT Id FROM Account", null, null);
            assertFalse(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }

        @ParameterizedTest
        @CsvSource({
            "INSERT INTO Account(Name) VALUES ('Test'), INSERT, true",
            "INSERT INTO Account(Name) VALUES ('Test'), SELECT, false",
            "INSERT INTO Account(Name) VALUES ('Test'), UPDATE, false",
            "INSERT INTO Account(Name) VALUES ('Test'), DELETE, false",
            "SELECT Id FROM Account, SELECT, true",
            "SELECT Id FROM Account, INSERT, false",
            "SELECT Id FROM Account, UPDATE, false",
            "SELECT Id FROM Account, DELETE, false",
            "UPDATE Account SET Name='Test', UPDATE, true",
            "UPDATE Account SET Name='Test', SELECT, false",
            "UPDATE Account SET Name='Test', INSERT, false",
            "UPDATE Account SET Name='Test', DELETE, false",
            "DELETE FROM Account WHERE Id='001', DELETE, true",
            "DELETE FROM Account WHERE Id='001', SELECT, false",
            "DELETE FROM Account WHERE Id='001', INSERT, false",
            "DELETE FROM Account WHERE Id='001', UPDATE, false"
        })
        @DisplayName("Should correctly match or not match statement types")
        void testAnalyseMatchesCorrectType(String soql, StatementTypeEnum type, boolean expected) {
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertEquals(expected, analyzer.analyse(soql, type));
        }
    }

    @Nested
    @DisplayName("Case insensitivity tests")
    class CaseInsensitivityTests {

        @ParameterizedTest
        @ValueSource(strings = {
            "SELECT Id FROM Account",
            "select Id from Account",
            "Select Id From Account",
            "SELECT id FROM account",
            "sElEcT Id FrOm Account"
        })
        @DisplayName("Should detect SELECT regardless of case")
        void testSelectCaseInsensitive(String soql) {
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }

        @ParameterizedTest
        @ValueSource(strings = {
            "INSERT INTO Account(Name) VALUES ('Test')",
            "insert into Account(Name) values ('Test')",
            "Insert Into Account(Name) Values ('Test')"
        })
        @DisplayName("Should detect INSERT regardless of case")
        void testInsertCaseInsensitive(String soql) {
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.INSERT));
        }

        @ParameterizedTest
        @ValueSource(strings = {
            "UPDATE Account SET Name='Test'",
            "update Account set Name='Test'",
            "Update Account Set Name='Test'"
        })
        @DisplayName("Should detect UPDATE regardless of case")
        void testUpdateCaseInsensitive(String soql) {
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.UPDATE));
        }

        @ParameterizedTest
        @ValueSource(strings = {
            "DELETE FROM Account WHERE Id='001'",
            "delete from Account where Id='001'",
            "Delete From Account Where Id='001'"
        })
        @DisplayName("Should detect DELETE regardless of case")
        void testDeleteCaseInsensitive(String soql) {
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.DELETE));
        }
    }

    @Nested
    @DisplayName("getType() method tests")
    class GetTypeTests {

        @Test
        @DisplayName("Should return SELECT for select queries")
        void testGetTypeSelect() {
            QueryAnalyzer analyzer = new QueryAnalyzer("SELECT Id FROM Account", null, null);
            assertEquals(StatementTypeEnum.SELECT, analyzer.getType());
        }

        @Test
        @DisplayName("Should return INSERT for insert queries")
        void testGetTypeInsert() {
            QueryAnalyzer analyzer = new QueryAnalyzer("INSERT INTO Account(Name) VALUES ('Test')", null, null);
            assertEquals(StatementTypeEnum.INSERT, analyzer.getType());
        }

        @Test
        @DisplayName("Should return UPDATE for update queries")
        void testGetTypeUpdate() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Account SET Name='Test'", null, null);
            assertEquals(StatementTypeEnum.UPDATE, analyzer.getType());
        }

        @Test
        @DisplayName("Should return DELETE for delete queries")
        void testGetTypeDelete() {
            QueryAnalyzer analyzer = new QueryAnalyzer("DELETE FROM Account WHERE Id='001'", null, null);
            assertEquals(StatementTypeEnum.DELETE, analyzer.getType());
        }

        @Test
        @DisplayName("Should return UNDEFINED for invalid SQL")
        void testGetTypeUndefined() {
            QueryAnalyzer analyzer = new QueryAnalyzer("INVALID SQL SYNTAX", null, null);
            assertEquals(StatementTypeEnum.UNDEFINED, analyzer.getType());
        }
    }

    @Nested
    @DisplayName("getFromObjectName() method tests")
    class GetFromObjectNameTests {

        @Test
        @DisplayName("Should get table name from SELECT query")
        void testGetFromObjectNameSelect() {
            QueryAnalyzer analyzer = new QueryAnalyzer("SELECT Id FROM Account", null, null);
            assertEquals("Account", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should get table name from INSERT query")
        void testGetFromObjectNameInsert() {
            QueryAnalyzer analyzer = new QueryAnalyzer("INSERT INTO Contact(Name) VALUES ('Test')", null, null);
            assertEquals("Contact", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should get table name from UPDATE query")
        void testGetFromObjectNameUpdate() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Opportunity SET Name='Test'", null, null);
            assertEquals("Opportunity", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should get table name from DELETE query")
        void testGetFromObjectNameDelete() {
            QueryAnalyzer analyzer = new QueryAnalyzer("DELETE FROM Lead WHERE Id='001'", null, null);
            assertEquals("Lead", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should return empty string for invalid SQL")
        void testGetFromObjectNameUndefined() {
            QueryAnalyzer analyzer = new QueryAnalyzer("INVALID SQL", null, null);
            assertEquals("", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should handle table names with underscores")
        void testGetFromObjectNameWithUnderscore() {
            QueryAnalyzer analyzer = new QueryAnalyzer("SELECT Id FROM Custom_Object__c", null, null);
            assertEquals("Custom_Object__c", analyzer.getFromObjectName());
        }
    }

    @Nested
    @DisplayName("getIdValue() method tests")
    class GetIdValueTests {

        @Test
        @DisplayName("Should extract Id value from WHERE Id='value'")
        void testGetIdValueFromWhereClause() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Account SET Name='Test' WHERE Id='001xx000003GeY0'", null, null);

            EqualsTo where = new EqualsTo();
            where.setLeftExpression(new Column("Id"));
            where.setRightExpression(new StringValue("001xx000003GeY0"));

            String idValue = analyzer.getIdValue(where, Collections.emptyList());
            assertEquals("001xx000003GeY0", idValue);
        }

        @Test
        @DisplayName("Should extract Id value when Id is on right side")
        void testGetIdValueIdOnRightSide() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Account SET Name='Test'", null, null);

            EqualsTo where = new EqualsTo();
            where.setLeftExpression(new StringValue("001xx000003GeY0"));
            where.setRightExpression(new Column("Id"));

            String idValue = analyzer.getIdValue(where, Collections.emptyList());
            assertEquals("001xx000003GeY0", idValue);
        }

        @Test
        @DisplayName("Should return null when WHERE clause has no Id column")
        void testGetIdValueNoIdColumn() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Account SET Name='Test'", null, null);

            EqualsTo where = new EqualsTo();
            where.setLeftExpression(new Column("Name"));
            where.setRightExpression(new StringValue("TestName"));

            String idValue = analyzer.getIdValue(where, Collections.emptyList());
            assertNull(idValue);
        }

        @Test
        @DisplayName("Should return null for null WHERE clause")
        void testGetIdValueNullWhere() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Account SET Name='Test'", null, null);
            assertNull(analyzer.getIdValue(null, Collections.emptyList()));
        }

        @Test
        @DisplayName("Should handle case insensitive Id column")
        void testGetIdValueCaseInsensitive() {
            QueryAnalyzer analyzer = new QueryAnalyzer("UPDATE Account SET Name='Test'", null, null);

            EqualsTo where = new EqualsTo();
            where.setLeftExpression(new Column("ID")); // uppercase
            where.setRightExpression(new StringValue("001xx000003GeY0"));

            String idValue = analyzer.getIdValue(where, Collections.emptyList());
            assertEquals("001xx000003GeY0", idValue);
        }
    }

    @Nested
    @DisplayName("Complex query tests")
    class ComplexQueryTests {

        @Test
        @DisplayName("Should handle SELECT with WHERE clause")
        void testSelectWithWhere() {
            String soql = "SELECT Id, Name FROM Account WHERE Name = 'Test'";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
            assertEquals("Account", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should handle SELECT with ORDER BY")
        void testSelectWithOrderBy() {
            String soql = "SELECT Id, Name FROM Account ORDER BY Name ASC";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }

        @Test
        @DisplayName("Should handle SELECT with LIMIT")
        void testSelectWithLimit() {
            String soql = "SELECT Id FROM Account LIMIT 100";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }

        @Test
        @DisplayName("Should handle INSERT with multiple values")
        void testInsertWithMultipleColumns() {
            String soql = "INSERT INTO Account(Name, Industry, Phone) VALUES ('Test', 'Tech', '123456')";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.INSERT));
            assertEquals("Account", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should handle UPDATE with multiple SET clauses")
        void testUpdateWithMultipleSetClauses() {
            String soql = "UPDATE Account SET Name='Test', Industry='Tech' WHERE Id='001'";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.UPDATE));
        }

        @Test
        @DisplayName("Should handle DELETE with complex WHERE")
        void testDeleteWithComplexWhere() {
            String soql = "DELETE FROM Account WHERE Id='001' AND Name='Test'";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.DELETE));
        }

        @Test
        @DisplayName("Should handle SELECT with alias")
        void testSelectWithAlias() {
            String soql = "SELECT a.Id, a.Name FROM Account a";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }
    }

    @Nested
    @DisplayName("Special characters and edge cases")
    class SpecialCharacterTests {

        @Test
        @DisplayName("Should handle query with special characters in values")
        void testSpecialCharactersInValues() {
            String soql = "INSERT INTO Account(Name) VALUES ('Test''s Account')";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.INSERT));
        }

        @Test
        @DisplayName("Should handle query with unicode characters")
        void testUnicodeCharacters() {
            String soql = "SELECT Id FROM Account WHERE Name = 'Test\u00e9'";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }

        @Test
        @DisplayName("Should handle query with underscores in object names")
        void testUnderscoresInObjectNames() {
            String soql = "SELECT Id FROM Custom_Object__c";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
            assertEquals("Custom_Object__c", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should handle query with numbers in column names")
        void testNumbersInColumnNames() {
            String soql = "SELECT Field1__c, Field2__c FROM Account";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.SELECT));
        }
    }

    @Nested
    @DisplayName("getSoql() method tests")
    class GetSoqlTests {

        @Test
        @DisplayName("Should return the original SOQL query")
        void testGetSoql() {
            String soql = "SELECT Id FROM Account";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertEquals(soql, analyzer.getSoql());
        }

        @Test
        @DisplayName("analyse() should update the SOQL query")
        void testAnalyseUpdatesSoql() {
            String originalSoql = "SELECT Id FROM Account";
            String newSoql = "SELECT Id FROM Contact";
            QueryAnalyzer analyzer = new QueryAnalyzer(originalSoql, null, null);

            analyzer.analyse(newSoql, StatementTypeEnum.SELECT);
            assertEquals(newSoql, analyzer.getSoql());
        }
    }

    @Nested
    @DisplayName("Parameter handling tests")
    class ParameterHandlingTests {

        @Test
        @DisplayName("Should handle JDBC parameter in WHERE clause")
        void testJdbcParameterInWhere() {
            String soql = "UPDATE Account SET Name=? WHERE Id=?";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.UPDATE));
        }

        @Test
        @DisplayName("Should handle multiple JDBC parameters")
        void testMultipleJdbcParameters() {
            String soql = "INSERT INTO Account(Name, Industry) VALUES (?, ?)";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertTrue(analyzer.analyse(soql, StatementTypeEnum.INSERT));
        }
    }

    @Nested
    @DisplayName("Invalid SQL handling tests")
    class InvalidSqlTests {

        @Test
        @DisplayName("Should handle completely invalid SQL")
        void testCompletelyInvalidSql() {
            String soql = "THIS IS NOT SQL";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertEquals(StatementTypeEnum.UNDEFINED, analyzer.getType());
            assertEquals("", analyzer.getFromObjectName());
        }

        @Test
        @DisplayName("Should handle partial SQL")
        void testPartialSql() {
            String soql = "SELECT FROM";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertEquals(StatementTypeEnum.UNDEFINED, analyzer.getType());
        }

        @Test
        @DisplayName("Should handle SQL with syntax errors")
        void testSqlWithSyntaxErrors() {
            String soql = "SELECT Id FRM Account"; // typo: FRM instead of FROM
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertEquals(StatementTypeEnum.UNDEFINED, analyzer.getType());
        }
    }

    @Nested
    @DisplayName("expandedStarSyntaxForFields flag tests")
    class ExpandedStarSyntaxTests {

        @Test
        @DisplayName("Should not set expandedStarSyntaxForFields for normal SELECT")
        void testExpandedStarSyntaxNotSetForNormalSelect() {
            String soql = "SELECT Id, Name FROM Account";
            QueryAnalyzer analyzer = new QueryAnalyzer(soql, null, null);
            assertFalse(analyzer.isExpandedStarSyntaxForFields());
        }
    }
}
