package com.ascendix.jdbc.salesforce.exceptions;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SalesforceExceptionsTest {

    @Nested
    @DisplayName("SalesforceJdbcException tests")
    class SalesforceJdbcExceptionTests {

        @Test
        @DisplayName("should extend SQLException")
        void testExtendsSQLException() {
            SalesforceJdbcException ex = new SalesforceJdbcException("test");
            assertTrue(ex instanceof SQLException);
        }

        @Test
        @DisplayName("should preserve message")
        void testMessage() {
            SalesforceJdbcException ex = new SalesforceJdbcException("test message");
            assertEquals("test message", ex.getMessage());
        }

        @Test
        @DisplayName("should preserve cause")
        void testCause() {
            Exception cause = new RuntimeException("cause");
            SalesforceJdbcException ex = new SalesforceJdbcException("test", cause);
            assertSame(cause, ex.getCause());
        }

        @Test
        @DisplayName("should construct from cause only")
        void testCauseOnlyConstructor() {
            Exception cause = new RuntimeException("original");
            SalesforceJdbcException ex = new SalesforceJdbcException(cause);
            assertSame(cause, ex.getCause());
        }
    }

    @Nested
    @DisplayName("SalesforceConnectionException tests")
    class SalesforceConnectionExceptionTests {

        @Test
        @DisplayName("should extend SalesforceJdbcException")
        void testExtendsSalesforceJdbcException() {
            SalesforceConnectionException ex = new SalesforceConnectionException("test");
            assertTrue(ex instanceof SalesforceJdbcException);
            assertTrue(ex instanceof SQLException);
        }

        @Test
        @DisplayName("should preserve message and cause")
        void testMessageAndCause() {
            Exception cause = new RuntimeException("network error");
            SalesforceConnectionException ex = new SalesforceConnectionException("Connection failed", cause);
            assertEquals("Connection failed", ex.getMessage());
            assertSame(cause, ex.getCause());
        }
    }

    @Nested
    @DisplayName("SalesforceQueryException tests")
    class SalesforceQueryExceptionTests {

        @Test
        @DisplayName("should extend SalesforceJdbcException")
        void testExtendsSalesforceJdbcException() {
            SalesforceQueryException ex = new SalesforceQueryException("test");
            assertTrue(ex instanceof SalesforceJdbcException);
            assertTrue(ex instanceof SQLException);
        }

        @Test
        @DisplayName("should preserve message and cause")
        void testMessageAndCause() {
            Exception cause = new RuntimeException("SOQL error");
            SalesforceQueryException ex = new SalesforceQueryException("Query failed", cause);
            assertEquals("Query failed", ex.getMessage());
            assertSame(cause, ex.getCause());
        }
    }

    @Nested
    @DisplayName("SalesforceRuntimeException tests")
    class SalesforceRuntimeExceptionTests {

        @Test
        @DisplayName("should extend RuntimeException")
        void testExtendsRuntimeException() {
            SalesforceRuntimeException ex = new SalesforceRuntimeException("test");
            assertTrue(ex instanceof RuntimeException);
            // SalesforceRuntimeException does NOT extend SQLException (checked by type system)
            assertFalse(SQLException.class.isAssignableFrom(SalesforceRuntimeException.class));
        }

        @Test
        @DisplayName("should preserve message and cause")
        void testMessageAndCause() {
            Exception cause = new SQLException("sql error");
            SalesforceRuntimeException ex = new SalesforceRuntimeException("Wrapper", cause);
            assertEquals("Wrapper", ex.getMessage());
            assertSame(cause, ex.getCause());
        }

        @Test
        @DisplayName("should be unchecked exception")
        void testUnchecked() {
            // Prove it's unchecked by catching it
            assertThrows(SalesforceRuntimeException.class, () -> {
                throw new SalesforceRuntimeException("unchecked");
            });
        }
    }

    @Nested
    @DisplayName("Exception hierarchy tests")
    class ExceptionHierarchyTests {

        @Test
        @DisplayName("SalesforceConnectionException can be caught as SQLException")
        void testConnectionExceptionAsSQLException() {
            try {
                throw new SalesforceConnectionException("connection error");
            } catch (SQLException e) {
                assertEquals("connection error", e.getMessage());
            }
        }

        @Test
        @DisplayName("SalesforceQueryException can be caught as SQLException")
        void testQueryExceptionAsSQLException() {
            try {
                throw new SalesforceQueryException("query error");
            } catch (SQLException e) {
                assertEquals("query error", e.getMessage());
            }
        }

        @Test
        @DisplayName("All JDBC exceptions can be caught as SalesforceJdbcException")
        void testCatchAsBase() {
            try {
                throw new SalesforceConnectionException("test");
            } catch (SalesforceJdbcException e) {
                assertTrue(e instanceof SalesforceConnectionException);
            }

            try {
                throw new SalesforceQueryException("test");
            } catch (SalesforceJdbcException e) {
                assertTrue(e instanceof SalesforceQueryException);
            }
        }
    }
}
