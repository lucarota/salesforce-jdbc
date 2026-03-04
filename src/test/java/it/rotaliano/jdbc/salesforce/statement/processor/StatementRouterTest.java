package it.rotaliano.jdbc.salesforce.statement.processor;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class StatementRouterTest {

    @Nested
    @DisplayName("isKeepAliveQuery tests")
    class IsKeepAliveQueryTests {

        @Test
        @DisplayName("should return true for keep alive query")
        void testKeepAliveQuery() {
            assertTrue(StatementRouter.isKeepAliveQuery("SELECT 'keep alive'"));
        }

        @Test
        @DisplayName("should return false for regular SELECT")
        void testRegularSelect() {
            assertFalse(StatementRouter.isKeepAliveQuery("SELECT Id FROM Account"));
        }

        @Test
        @DisplayName("should return false for null")
        void testNull() {
            assertFalse(StatementRouter.isKeepAliveQuery(null));
        }
    }

    @Nested
    @DisplayName("isAdminQuery tests")
    class IsAdminQueryTests {

        @ParameterizedTest
        @DisplayName("should detect CONNECT admin queries")
        @ValueSource(strings = {
            "CONNECT INFO",
            "CONNECT USER 'test' IDENTIFIED BY 'pass'",
            "CONNECT TO jdbc:salesforce://login.salesforce.com USER 'test' IDENTIFIED BY 'pass'",
            "CONN test/pass"
        })
        void testAdminQueries(String query) {
            assertTrue(StatementRouter.isAdminQuery(query));
        }

        @ParameterizedTest
        @DisplayName("should not detect regular queries as admin")
        @ValueSource(strings = {
            "SELECT Id FROM Account",
            "INSERT INTO Account(Name) VALUES('Test')",
            "UPDATE Account SET Name='Test' WHERE Id='001'",
            "DELETE FROM Account WHERE Id='001'",
            "CACHE GLOBAL SELECT Id FROM Account"  // CACHE is NOT an admin query, handled separately
        })
        void testNonAdminQueries(String query) {
            assertFalse(StatementRouter.isAdminQuery(query));
        }
    }

    @Nested
    @DisplayName("getStatementType tests")
    class GetStatementTypeTests {

        @Test
        @DisplayName("should return SELECT for keep alive query")
        void testKeepAliveType() {
            assertEquals(StatementTypeEnum.SELECT,
                StatementRouter.getStatementType("SELECT 'keep alive'", null));
        }

        @Test
        @DisplayName("should return SELECT for CONNECT admin query")
        void testAdminQueryType() {
            assertEquals(StatementTypeEnum.SELECT,
                StatementRouter.getStatementType("CONNECT INFO", null));
        }

        @ParameterizedTest
        @DisplayName("should return UNDEFINED for null or empty queries")
        @NullAndEmptySource
        @ValueSource(strings = {"   ", "\t", "\n"})
        void testNullOrEmptyType(String query) {
            assertEquals(StatementTypeEnum.UNDEFINED,
                StatementRouter.getStatementType(query, null));
        }

        @Test
        @DisplayName("should return SELECT when no analyzer provided")
        void testSelectWithoutAnalyzer() {
            assertEquals(StatementTypeEnum.SELECT,
                StatementRouter.getStatementType("SELECT Id FROM Account", null));
        }

        @Test
        @DisplayName("should detect INSERT with analyzer")
        void testInsertType() {
            QueryAnalyzer analyzer = mock(QueryAnalyzer.class);
            String query = "INSERT INTO Account(Name) VALUES('Test')";
            when(analyzer.analyse(query, StatementTypeEnum.INSERT)).thenReturn(true);

            assertEquals(StatementTypeEnum.INSERT,
                StatementRouter.getStatementType(query, analyzer));
        }

        @Test
        @DisplayName("should detect UPDATE with analyzer")
        void testUpdateType() {
            QueryAnalyzer analyzer = mock(QueryAnalyzer.class);
            String query = "UPDATE Account SET Name='Test' WHERE Id='001'";
            when(analyzer.analyse(query, StatementTypeEnum.UPDATE)).thenReturn(true);

            assertEquals(StatementTypeEnum.UPDATE,
                StatementRouter.getStatementType(query, analyzer));
        }

        @Test
        @DisplayName("should detect DELETE with analyzer")
        void testDeleteType() {
            QueryAnalyzer analyzer = mock(QueryAnalyzer.class);
            String query = "DELETE FROM Account WHERE Id='001'";
            when(analyzer.analyse(query, StatementTypeEnum.DELETE)).thenReturn(true);

            assertEquals(StatementTypeEnum.DELETE,
                StatementRouter.getStatementType(query, analyzer));
        }
    }

    @Nested
    @DisplayName("isDmlStatement tests")
    class IsDmlStatementTests {

        @Test
        @DisplayName("should return true for INSERT")
        void testInsertIsDml() {
            QueryAnalyzer analyzer = mock(QueryAnalyzer.class);
            String query = "INSERT INTO Account(Name) VALUES('Test')";
            when(analyzer.analyse(query, StatementTypeEnum.INSERT)).thenReturn(true);

            assertTrue(StatementRouter.isDmlStatement(query, analyzer));
        }

        @Test
        @DisplayName("should return false for SELECT")
        void testSelectIsNotDml() {
            QueryAnalyzer analyzer = mock(QueryAnalyzer.class);
            String query = "SELECT Id FROM Account";

            assertFalse(StatementRouter.isDmlStatement(query, analyzer));
        }

        @Test
        @DisplayName("should return false for admin queries")
        void testAdminIsNotDml() {
            assertFalse(StatementRouter.isDmlStatement("CONNECT INFO", null));
        }
    }

    @Nested
    @DisplayName("isSelectStatement tests")
    class IsSelectStatementTests {

        @Test
        @DisplayName("should return true for SELECT query")
        void testSelectIsSelect() {
            assertTrue(StatementRouter.isSelectStatement("SELECT Id FROM Account", null));
        }

        @Test
        @DisplayName("should return true for keep alive query")
        void testKeepAliveIsSelect() {
            assertTrue(StatementRouter.isSelectStatement("SELECT 'keep alive'", null));
        }

        @Test
        @DisplayName("should return false for INSERT")
        void testInsertIsNotSelect() {
            QueryAnalyzer analyzer = mock(QueryAnalyzer.class);
            String query = "INSERT INTO Account(Name) VALUES('Test')";
            when(analyzer.analyse(query, StatementTypeEnum.INSERT)).thenReturn(true);

            assertFalse(StatementRouter.isSelectStatement(query, analyzer));
        }
    }
}
