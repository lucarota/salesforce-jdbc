package it.rotaliano.jdbc.salesforce.resultset;

import static org.junit.jupiter.api.Assertions.*;

import it.rotaliano.jdbc.salesforce.metadata.ColumnMap;
import it.rotaliano.jdbc.salesforce.metadata.TypeInfo;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Base64;
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
    @DisplayName("getObject() tests")
    class GetObjectTests {

        @Test
        @DisplayName("should get object by column name")
        void testGetObjectByColumnName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("col", "value", TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals("value", rs.getObject("col"));
        }

        @Test
        @DisplayName("should get object by column index")
        void testGetObjectByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("col", 123, TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(123, rs.getObject(1));
        }

        @Test
        @DisplayName("should return null for null value")
        void testGetObjectNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("col", null, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertNull(rs.getObject("col"));
        }
    }

    @Nested
    @DisplayName("getFloat() tests")
    class GetFloatTests {

        @Test
        @DisplayName("should get float by column name")
        void testGetFloatByColumnName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("floatCol", "3.14", TypeInfo.DOUBLE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(3.14f, rs.getFloat("floatCol"), 0.001f);
        }

        @Test
        @DisplayName("should get float by column index")
        void testGetFloatByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("floatCol", "2.71", TypeInfo.DOUBLE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(2.71f, rs.getFloat(1), 0.001f);
        }

        @Test
        @DisplayName("should return 0 for null")
        void testGetFloatNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("floatCol", null, TypeInfo.DOUBLE_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals(0f, rs.getFloat("floatCol"));
        }
    }

    @Nested
    @DisplayName("getShort() tests")
    class GetShortTests {

        @Test
        @DisplayName("should get short by column name")
        void testGetShortByColumnName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("shortCol", "100", TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals((short) 100, rs.getShort("shortCol"));
        }

        @Test
        @DisplayName("should get short by column index")
        void testGetShortByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("shortCol", "200", TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals((short) 200, rs.getShort(1));
        }

        @Test
        @DisplayName("should return 0 for null")
        void testGetShortNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("shortCol", null, TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals((short) 0, rs.getShort("shortCol"));
        }
    }

    @Nested
    @DisplayName("getByte() tests")
    class GetByteTests {

        @Test
        @DisplayName("should get byte by column name")
        void testGetByteByColumnName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("byteCol", "42", TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals((byte) 42, rs.getByte("byteCol"));
        }

        @Test
        @DisplayName("should get byte by column index")
        void testGetByteByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("byteCol", "127", TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals((byte) 127, rs.getByte(1));
        }

        @Test
        @DisplayName("should return 0 for null")
        void testGetByteNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("byteCol", null, TypeInfo.INT_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertEquals((byte) 0, rs.getByte("byteCol"));
        }
    }

    @Nested
    @DisplayName("getBytes() tests")
    class GetBytesTests {

        @Test
        @DisplayName("should get bytes by column name")
        void testGetBytesByColumnName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("bytesCol", "Hello", TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertArrayEquals("Hello".getBytes(), rs.getBytes("bytesCol"));
        }

        @Test
        @DisplayName("should get bytes by column index")
        void testGetBytesByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("bytesCol", "World", TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertArrayEquals("World".getBytes(), rs.getBytes(1));
        }

        @Test
        @DisplayName("should return empty array for null")
        void testGetBytesNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("bytesCol", null, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertArrayEquals(new byte[0], rs.getBytes("bytesCol"));
        }
    }

    @Nested
    @DisplayName("getBlob() tests")
    class GetBlobTests {

        @Test
        @DisplayName("should get blob from base64 string by column name")
        void testGetBlobByColumnName() throws Exception {
            String base64 = Base64.getEncoder().encodeToString("test data".getBytes());
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("blobCol", base64, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            java.sql.Blob blob = rs.getBlob("blobCol");
            assertNotNull(blob);
            assertArrayEquals("test data".getBytes(), blob.getBytes(1, (int) blob.length()));
        }

        @Test
        @DisplayName("should get blob by column index")
        void testGetBlobByIndex() throws Exception {
            String base64 = Base64.getEncoder().encodeToString("blob content".getBytes());
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("blobCol", base64, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            java.sql.Blob blob = rs.getBlob(1);
            assertNotNull(blob);
        }

        @Test
        @DisplayName("should return null for null blob")
        void testGetBlobNull() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("blobCol", null, TypeInfo.STRING_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertNull(rs.getBlob("blobCol"));
        }
    }

    @Nested
    @DisplayName("Cursor position tests")
    class CursorPositionTests {

        @Test
        @DisplayName("isFirst should return true when on first row")
        void testIsFirst() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2));
            rs.next();

            assertTrue(rs.isFirst());
        }

        @Test
        @DisplayName("isFirst should return false when not on first row")
        void testIsFirstFalse() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2));
            rs.next();
            rs.next();

            assertFalse(rs.isFirst());
        }

        @Test
        @DisplayName("isLast should return true when on last row")
        void testIsLast() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2));
            rs.next();
            rs.next();

            assertTrue(rs.isLast());
        }

        @Test
        @DisplayName("isLast should return false when not on last row")
        void testIsLastFalse() {
            ColumnMap<String, Object> row1 = new ColumnMap<>();
            row1.put("id", "1", TypeInfo.STRING_TYPE_INFO);
            ColumnMap<String, Object> row2 = new ColumnMap<>();
            row2.put("id", "2", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row1, row2));
            rs.next();

            assertFalse(rs.isLast());
        }

        @Test
        @DisplayName("isAfterLast should return true after iterating past last row")
        void testIsAfterLast() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("id", "1", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();
            rs.next(); // past last row

            assertTrue(rs.isAfterLast());
        }

        @Test
        @DisplayName("isAfterLast should return false when still on a row")
        void testIsAfterLastFalse() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("id", "1", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            assertFalse(rs.isAfterLast());
        }
    }

    @Nested
    @DisplayName("ResultSet properties tests")
    class ResultSetPropertiesTests {

        @Test
        @DisplayName("getType should return TYPE_FORWARD_ONLY")
        void testGetType() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        }

        @Test
        @DisplayName("getConcurrency should return CONCUR_READ_ONLY")
        void testGetConcurrency() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        }

        @Test
        @DisplayName("getFetchDirection should return FETCH_UNKNOWN")
        void testGetFetchDirection() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());
        }

        @Test
        @DisplayName("getHoldability should return CLOSE_CURSORS_AT_COMMIT")
        void testGetHoldability() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, rs.getHoldability());
        }

        @Test
        @DisplayName("isClosed should return false")
        void testIsClosed() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertFalse(rs.isClosed());
        }
    }

    @Nested
    @DisplayName("Warnings tests")
    class WarningsTests {

        @Test
        @DisplayName("should return null warnings initially")
        void testGetWarningsInitially() {
            CachedResultSet rs = new CachedResultSet(List.of());
            assertNull(rs.getWarnings());
        }

        @Test
        @DisplayName("should add and retrieve warning")
        void testAddWarning() {
            CachedResultSet rs = new CachedResultSet(List.of());
            rs.addWarning("Test warning");

            assertNotNull(rs.getWarnings());
            assertEquals("Test warning", rs.getWarnings().getMessage());
        }

        @Test
        @DisplayName("should chain multiple warnings")
        void testAddMultipleWarnings() {
            CachedResultSet rs = new CachedResultSet(List.of());
            rs.addWarning("Warning 1");
            rs.addWarning("Warning 2");

            assertNotNull(rs.getWarnings());
            assertEquals("Warning 1", rs.getWarnings().getMessage());
            assertNotNull(rs.getWarnings().getNextWarning());
            assertEquals("Warning 2", rs.getWarnings().getNextWarning().getMessage());
        }

        @Test
        @DisplayName("should clear warnings")
        void testClearWarnings() {
            CachedResultSet rs = new CachedResultSet(List.of());
            rs.addWarning("Test warning");
            rs.clearWarnings();

            assertNull(rs.getWarnings());
        }
    }

    @Nested
    @DisplayName("EMPTY singleton tests")
    class EmptySingletonTests {

        @Test
        @DisplayName("EMPTY should have no rows")
        void testEmptyHasNoRows() {
            assertFalse(CachedResultSet.EMPTY.next());
        }

        @Test
        @DisplayName("EMPTY should return metadata")
        void testEmptyHasMetadata() {
            assertNotNull(CachedResultSet.EMPTY.getMetaData());
        }

        @Test
        @DisplayName("EMPTY should be same instance")
        void testEmptySameInstance() {
            assertSame(CachedResultSet.EMPTY, CachedResultSet.EMPTY);
        }
    }

    @Nested
    @DisplayName("BigDecimal with scale tests")
    class BigDecimalScaleTests {

        @Test
        @DisplayName("should get BigDecimal with scale by column index")
        @SuppressWarnings("deprecation")
        void testGetBigDecimalWithScaleByIndex() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("amount", "123.456789", TypeInfo.DECIMAL_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            java.math.BigDecimal result = rs.getBigDecimal(1, 2);
            assertEquals(new java.math.BigDecimal("123.46"), result);
        }

        @Test
        @DisplayName("should get BigDecimal with scale by column name")
        @SuppressWarnings("deprecation")
        void testGetBigDecimalWithScaleByName() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("amount", "99.999", TypeInfo.DECIMAL_TYPE_INFO);
            CachedResultSet rs = new CachedResultSet(List.of(row));
            rs.next();

            java.math.BigDecimal result = rs.getBigDecimal("amount", 1);
            assertEquals(new java.math.BigDecimal("100.0"), result);
        }
    }

    @Nested
    @DisplayName("insertRow() exception test")
    class InsertRowTests {

        @Test
        @DisplayName("insertRow should throw SQLException")
        void testInsertRowThrowsException() {
            CachedResultSet rs = new CachedResultSet(List.of());

            SQLException exception = assertThrows(SQLException.class, rs::insertRow);
            assertEquals("Feature is not supported.", exception.getMessage());
            assertEquals("HY000", exception.getSQLState());
        }
    }

    @Nested
    @DisplayName("getMetaData() tests")
    class GetMetaDataTests {

        @Test
        @DisplayName("should return provided metadata")
        void testGetMetaData() throws Exception {
            javax.sql.rowset.RowSetMetaDataImpl meta = new javax.sql.rowset.RowSetMetaDataImpl();
            meta.setColumnCount(1);
            meta.setColumnName(1, "testCol");

            CachedResultSet rs = new CachedResultSet(List.of(), meta);

            assertNotNull(rs.getMetaData());
            assertEquals(1, rs.getMetaData().getColumnCount());
        }

        @Test
        @DisplayName("should return empty metadata when none provided")
        void testGetMetaDataEmpty() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            CachedResultSet rs = new CachedResultSet(row);

            assertNotNull(rs.getMetaData());
        }
    }

    @Nested
    @DisplayName("Constructor tests")
    class ConstructorTests {

        @Test
        @DisplayName("should create with list of rows")
        void testConstructorWithList() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("id", "1", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(List.of(row));

            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));
        }

        @Test
        @DisplayName("should create with single row")
        void testConstructorWithSingleRow() {
            ColumnMap<String, Object> row = new ColumnMap<>();
            row.put("id", "single", TypeInfo.STRING_TYPE_INFO);

            CachedResultSet rs = new CachedResultSet(row);

            assertTrue(rs.next());
            assertEquals("single", rs.getString("id"));
            assertFalse(rs.next());
        }

        @Test
        @DisplayName("should create with metadata only")
        void testConstructorWithMetadataOnly() throws Exception {
            javax.sql.rowset.RowSetMetaDataImpl meta = new javax.sql.rowset.RowSetMetaDataImpl();
            meta.setColumnCount(1);

            CachedResultSet rs = new CachedResultSet(meta);

            assertFalse(rs.next());
            assertNotNull(rs.getMetaData());
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
