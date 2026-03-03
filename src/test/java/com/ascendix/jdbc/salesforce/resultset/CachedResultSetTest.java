package com.ascendix.jdbc.salesforce.resultset;

import static org.junit.jupiter.api.Assertions.*;

import com.ascendix.jdbc.salesforce.metadata.ColumnMap;
import com.ascendix.jdbc.salesforce.metadata.TypeInfo;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CachedResultSetTest {

    private CachedResultSet cachedResultSet;

    @BeforeEach
    void setUp() {
        ColumnMap<String, Object> columnMap = new ColumnMap<>();
        cachedResultSet = new CachedResultSet(columnMap);
    }

    @Nested
    @DisplayName("parseDate() tests")
    class ParseDateTests {

        @Test
        @DisplayName("should parse valid date string")
        void testParseDate() {
            java.util.Date actual = cachedResultSet.parseDate("2017-06-23");

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(actual);

            assertEquals(2017, calendar.get(Calendar.YEAR));
            assertEquals(Calendar.JUNE, calendar.get(Calendar.MONTH));
            assertEquals(23, calendar.get(Calendar.DAY_OF_MONTH));
        }

        @Test
        @DisplayName("should parse date at year boundary")
        void testParseDateYearBoundary() {
            java.util.Date actual = cachedResultSet.parseDate("2023-12-31");

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(actual);

            assertEquals(2023, calendar.get(Calendar.YEAR));
            assertEquals(Calendar.DECEMBER, calendar.get(Calendar.MONTH));
            assertEquals(31, calendar.get(Calendar.DAY_OF_MONTH));
        }

        @Test
        @DisplayName("should parse date at beginning of year")
        void testParseDateBeginningOfYear() {
            java.util.Date actual = cachedResultSet.parseDate("2024-01-01");

            Calendar calendar = Calendar.getInstance();
            calendar.setTime(actual);

            assertEquals(2024, calendar.get(Calendar.YEAR));
            assertEquals(Calendar.JANUARY, calendar.get(Calendar.MONTH));
            assertEquals(1, calendar.get(Calendar.DAY_OF_MONTH));
        }

        @Test
        @DisplayName("should return null for null input")
        void testParseDateNull() {
            java.util.Date actual = cachedResultSet.parseDate(null);
            assertNull(actual);
            assertTrue(cachedResultSet.wasNull());
        }

        @ParameterizedTest
        @DisplayName("should return null for invalid date strings")
        // Note: SimpleDateFormat is lenient by default and very permissive
        // Formats like "2023-13-01" or "23-06-2023" get parsed incorrectly
        // This will be fixed when migrating to DateTimeFormatter
        @ValueSource(strings = {"", "invalid", "2023/06/23", "not-a-date"})
        void testParseDateInvalid(String invalidDate) {
            java.util.Date actual = cachedResultSet.parseDate(invalidDate);
            assertNull(actual);
        }
    }

    @Nested
    @DisplayName("getDate() tests")
    class GetDateTests {

        @Test
        @DisplayName("should get date by column name")
        void testGetDateByColumnName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("dateColumn", "2023-07-15", TypeInfo.DATE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Date result = rs.getDate("dateColumn");

            assertNotNull(result);
            Calendar cal = Calendar.getInstance();
            cal.setTime(result);
            assertEquals(2023, cal.get(Calendar.YEAR));
            assertEquals(Calendar.JULY, cal.get(Calendar.MONTH));
            assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));
        }

        @Test
        @DisplayName("should get date by column index")
        void testGetDateByColumnIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("dateColumn", "2023-07-15", TypeInfo.DATE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Date result = rs.getDate(1);

            assertNotNull(result);
            Calendar cal = Calendar.getInstance();
            cal.setTime(result);
            assertEquals(2023, cal.get(Calendar.YEAR));
        }

        @Test
        @DisplayName("should return null for null value")
        void testGetDateNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("dateColumn", null, TypeInfo.DATE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Date result = rs.getDate("dateColumn");

            assertNull(result);
            assertTrue(rs.wasNull());
        }
    }

    @Nested
    @DisplayName("getTimestamp() tests")
    class GetTimestampTests {

        @Test
        @DisplayName("should get timestamp from datetime string")
        void testGetTimestampFromString() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("tsColumn", "2023-07-15T14:30:45.123Z", TypeInfo.DATETIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp("tsColumn");

            assertNotNull(result);
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            cal.setTime(result);
            assertEquals(2023, cal.get(Calendar.YEAR));
            assertEquals(Calendar.JULY, cal.get(Calendar.MONTH));
            assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));
            assertEquals(14, cal.get(Calendar.HOUR_OF_DAY));
            assertEquals(30, cal.get(Calendar.MINUTE));
            assertEquals(45, cal.get(Calendar.SECOND));
        }

        @Test
        @DisplayName("should get timestamp from GregorianCalendar")
        void testGetTimestampFromGregorianCalendar() {
            GregorianCalendar gc = new GregorianCalendar(2023, Calendar.AUGUST, 20, 10, 15, 30);
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("tsColumn", gc, TypeInfo.DATETIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp("tsColumn");

            assertNotNull(result);
            assertEquals(gc.getTimeInMillis(), result.getTime());
        }

        @Test
        @DisplayName("should get timestamp by column index")
        void testGetTimestampByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("tsColumn", "2023-07-15T14:30:45.123Z", TypeInfo.DATETIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp(1);

            assertNotNull(result);
        }

        @Test
        @DisplayName("should get timestamp from GregorianCalendar by index")
        void testGetTimestampFromGregorianCalendarByIndex() {
            GregorianCalendar gc = new GregorianCalendar(2023, Calendar.AUGUST, 20, 10, 15, 30);
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("tsColumn", gc, TypeInfo.DATETIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp(1);

            assertNotNull(result);
            assertEquals(gc.getTimeInMillis(), result.getTime());
        }

        @Test
        @DisplayName("should return null for null value")
        void testGetTimestampNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("tsColumn", null, TypeInfo.DATETIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp("tsColumn");

            assertNull(result);
            assertTrue(rs.wasNull());
        }

        @Test
        @DisplayName("should handle DATE type info - parse as date only")
        void testGetTimestampWithDateTypeInfo() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("dateColumn", "2023-07-15", TypeInfo.DATE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp("dateColumn");

            assertNotNull(result);
            Calendar cal = Calendar.getInstance();
            cal.setTime(result);
            assertEquals(2023, cal.get(Calendar.YEAR));
            assertEquals(Calendar.JULY, cal.get(Calendar.MONTH));
            assertEquals(15, cal.get(Calendar.DAY_OF_MONTH));
        }

        @Test
        @DisplayName("should handle DATE type info by index")
        void testGetTimestampWithDateTypeInfoByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("dateColumn", "2023-07-15", TypeInfo.DATE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Timestamp result = rs.getTimestamp(1);

            assertNotNull(result);
            Calendar cal = Calendar.getInstance();
            cal.setTime(result);
            assertEquals(2023, cal.get(Calendar.YEAR));
        }
    }

    @Nested
    @DisplayName("getTime() tests")
    class GetTimeTests {

        @Test
        @DisplayName("should get time from time string")
        void testGetTimeFromString() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("timeColumn", "14:30:45.123Z", TypeInfo.TIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Time result = rs.getTime("timeColumn");

            assertNotNull(result);
        }

        @Test
        @DisplayName("should get time by column index")
        void testGetTimeByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("timeColumn", "14:30:45.123Z", TypeInfo.TIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Time result = rs.getTime(1);

            assertNotNull(result);
        }

        @Test
        @DisplayName("should return null for null value")
        void testGetTimeNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("timeColumn", null, TypeInfo.TIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Time result = rs.getTime("timeColumn");

            assertNull(result);
            assertTrue(rs.wasNull());
        }

        @ParameterizedTest
        @DisplayName("should return null for invalid time strings")
        // Note: SimpleDateFormat is lenient by default, so "25:00:00.000Z" would be parsed as 01:00:00 next day
        // This will be fixed when migrating to DateTimeFormatter
        @ValueSource(strings = {"", "invalid", "not-a-time"})
        void testGetTimeInvalid(String invalidTime) {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("timeColumn", invalidTime, TypeInfo.TIME_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            Time result = rs.getTime("timeColumn");

            assertNull(result);
        }
    }

    @Nested
    @DisplayName("Navigation tests")
    class NavigationTests {

        @Test
        @DisplayName("should navigate through multiple rows")
        void testNextMultipleRows() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row3 = new ColumnMap<>();
            row3.put("id", "3", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2, row3));

            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));
            assertTrue(rs.next());
            assertEquals("2", rs.getString("id"));
            assertTrue(rs.next());
            assertEquals("3", rs.getString("id"));
            assertFalse(rs.next());
        }

        @Test
        @DisplayName("should return false for empty result set")
        void testNextEmpty() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertFalse(rs.next());
        }

        @Test
        @DisplayName("should navigate to first row")
        void testFirst() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2));
            rs.next();
            rs.next();

            assertTrue(rs.first());
            assertEquals("1", rs.getString("id"));
        }

        @Test
        @DisplayName("should navigate to last row")
        void testLast() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2));

            assertTrue(rs.last());
            assertEquals("2", rs.getString("id"));
        }

        @Test
        @DisplayName("should reset to before first")
        void testBeforeFirst() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1));
            rs.next();

            rs.beforeFirst();
            assertTrue(rs.isBeforeFirst());
            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));
        }
    }

    @Nested
    @DisplayName("Type conversion tests")
    class TypeConversionTests {

        @Test
        @DisplayName("should get string value")
        void testGetString() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("name", "Test Value", TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals("Test Value", rs.getString("name"));
            assertEquals("Test Value", rs.getString(1));
        }

        @Test
        @DisplayName("should convert number to string")
        void testGetStringFromNumber() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("number", 12345, TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals("12345", rs.getString("number"));
        }

        @Test
        @DisplayName("should get int value")
        void testGetInt() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("count", "42", TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(42, rs.getInt("count"));
            assertEquals(42, rs.getInt(1));
        }

        @Test
        @DisplayName("should get long value")
        void testGetLong() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("bigNum", "9223372036854775807", TypeInfo.LONG_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(Long.MAX_VALUE, rs.getLong("bigNum"));
        }

        @Test
        @DisplayName("should get double value")
        void testGetDouble() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("decimal", "3.14159", TypeInfo.DOUBLE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(3.14159, rs.getDouble("decimal"), 0.00001);
        }

        @Test
        @DisplayName("should get boolean value")
        void testGetBoolean() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("flag", "true", TypeInfo.BOOL_TYPE_INFO);
            row.put("flag2", "false", TypeInfo.BOOL_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertTrue(rs.getBoolean("flag"));
            assertFalse(rs.getBoolean("flag2"));
        }

        @Test
        @DisplayName("should get BigDecimal value")
        void testGetBigDecimal() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("amount", "12345.67", TypeInfo.DECIMAL_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(new java.math.BigDecimal("12345.67"), rs.getBigDecimal("amount"));
        }

        @Test
        @DisplayName("should return default values for null")
        void testDefaultValuesForNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("nullVal", null, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(0, rs.getInt("nullVal"));
            assertEquals(0L, rs.getLong("nullVal"));
            assertEquals(0.0, rs.getDouble("nullVal"));
            assertFalse(rs.getBoolean("nullVal"));
            assertNull(rs.getString("nullVal"));
        }
    }

    @Nested
    @DisplayName("wasNull() tests")
    class WasNullTests {

        @Test
        @DisplayName("wasNull should return true after reading null value")
        void testWasNullTrue() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("nullColumn", null, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            rs.getString("nullColumn");
            assertTrue(rs.wasNull());
        }

        @Test
        @DisplayName("wasNull should return false after reading non-null value")
        void testWasNullFalse() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("column", "value", TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            rs.getString("column");
            assertFalse(rs.wasNull());
        }
    }

    @Nested
    @DisplayName("Thread safety contract tests")
    class ThreadSafetyContractTests {

        @Test
        @DisplayName("CachedResultSet is NOT thread-safe by design (JDBC spec compliant)")
        void testNotThreadSafeByDesign() {
            // This test documents that CachedResultSet is NOT thread-safe,
            // which is consistent with the JDBC specification.
            //
            // The JDBC spec does not require ResultSet to be thread-safe.
            // If concurrent access is needed, the application must provide
            // external synchronization.
            //
            // Shared mutable state includes:
            // - index (cursor position)
            // - wasNull flag
            // - rows list (when using rowSupplier)

            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));

            // Single-threaded usage works correctly
            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));
            assertFalse(rs.wasNull());

            // For multi-threaded scenarios, wrap with external synchronization:
            // synchronized(rs) { rs.next(); value = rs.getString("id"); }
        }

        @Test
        @DisplayName("Single thread sequential access is safe")
        void testSingleThreadSequentialAccess() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row3 = new ColumnMap<>();
            row3.put("id", "3", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2, row3));

            // Sequential navigation and reads are safe in single thread
            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));

            assertTrue(rs.next());
            assertEquals("2", rs.getString("id"));

            assertTrue(rs.next());
            assertEquals("3", rs.getString("id"));

            // Can reset and iterate again
            rs.beforeFirst();
            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));
        }
    }
}
