package com.ascendix.jdbc.salesforce.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;

import com.ascendix.jdbc.salesforce.connection.ForceConnection;
import com.ascendix.jdbc.salesforce.statement.ForcePreparedStatement.CacheMode;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.List;
import org.junit.jupiter.api.Test;

class ForcePreparedStatementTest {

    private final ForceConnection connection;

    ForcePreparedStatementTest() {
        connection = mock(ForceConnection.class);
    }

    @Test
    void testGetParamClass() {
        assertEquals(String.class, ForcePreparedStatement.getParamClass("test"));
        assertEquals(Long.class, ForcePreparedStatement.getParamClass(1L));
        assertEquals(Object.class, ForcePreparedStatement.getParamClass(new SimpleDateFormat()));
        assertNull(ForcePreparedStatement.getParamClass(null));
    }

    @Test
    void testToSoqlStringParam() {
        assertEquals("'\\''", ForcePreparedStatement.toSoqlStringParam("'"));
        assertEquals("'\\\\'", ForcePreparedStatement.toSoqlStringParam("\\"));
        assertEquals("'\\';DELETE DATABASE \\\\a'", ForcePreparedStatement.toSoqlStringParam("';DELETE DATABASE \\a"));
    }

    @Test
    void testConvertToSoqlParam() {
        assertEquals("123.45", ForcePreparedStatement.convertToSoqlParam(123.45));
        assertEquals("123.45", ForcePreparedStatement.convertToSoqlParam(123.45f));
        assertEquals("123", ForcePreparedStatement.convertToSoqlParam(123L));
        assertEquals("123.45", ForcePreparedStatement.convertToSoqlParam(new BigDecimal("123.45")));
        assertEquals("2017-03-06T12:34:56+0100", ForcePreparedStatement.convertToSoqlParam(new GregorianCalendar(2017, 2, 6, 12, 34, 56).getTime()));
        assertEquals("'\\'test\\'\\\\'", ForcePreparedStatement.convertToSoqlParam("'test'\\"));
        assertEquals("NULL", ForcePreparedStatement.convertToSoqlParam(null));
    }

    @Test
    void testAddParameter() throws SQLException {
        ForcePreparedStatement statement = new ForcePreparedStatement(connection, "");
        statement.addParameter(1, "one");
        statement.addParameter(3, "two");

        List<Object> actual = statement.getParameters();

        assertEquals(3, actual.size());
        assertEquals("one", actual.get(0));
        assertEquals("two", actual.get(2));
        assertNull(actual.get(1));
    }

    @Test
    void testSetParams() throws SQLException {
        ForcePreparedStatement statement = new ForcePreparedStatement(connection, "");
        String query = "SELECT Something FROM Anything WERE name = ? AND age > ?";
        statement.addParameter(1, "one");
        statement.addParameter(2, 123);

        String actual = statement.setParams(query);

        assertEquals("SELECT Something FROM Anything WERE name = 'one' AND age > 123", actual);
    }


    @Test
    void testGetCacheMode() throws Exception {
        ForcePreparedStatement statement = new ForcePreparedStatement(connection, "");


        assertEquals(ForcePreparedStatement.CacheMode.SESSION, getCacheMode(statement, "CACHE SESSION select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.GLOBAL, getCacheMode(statement, " Cache global select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.NO_CACHE, getCacheMode(statement,"select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.NO_CACHE, getCacheMode(statement," Cache unknown select name from Account"));
    }

    @Test
    void removeCacheHints() throws Exception {
        ForcePreparedStatement statement = new ForcePreparedStatement(connection, "");
        assertEquals("   select   name   from Account  ", getSqlAfterRemoveCacheHints(statement, " Cache global  select   name   from Account  "));
        assertEquals("  select name from Account", getSqlAfterRemoveCacheHints(statement, " Cache SesSioN select name from Account"));
        assertNotEquals("  select name from Account", getSqlAfterRemoveCacheHints(statement, " Cache other select name from Account"));
    }

    private static CacheMode getCacheMode(ForcePreparedStatement statement, String query) throws Exception {
        final Class<? extends ForcePreparedStatement> stClass = statement.getClass();
        final Method setCacheMode = stClass.getDeclaredMethod("setCacheMode", String.class);
        setCacheMode.setAccessible(true);
        setCacheMode.invoke(statement, query);

        Field field = stClass.getDeclaredField("cacheMode");
        field.setAccessible(true);
        return (CacheMode) field.get(statement);
    }

    private static String getSqlAfterRemoveCacheHints(ForcePreparedStatement statement, String query) throws Exception {
        final Class<? extends ForcePreparedStatement> stClass = statement.getClass();
        final Method setCacheMode = stClass.getDeclaredMethod("setCacheMode", String.class);
        setCacheMode.setAccessible(true);
        setCacheMode.invoke(statement, query);

        Field field = stClass.getDeclaredField("soqlQuery");
        field.setAccessible(true);
        return (String) field.get(statement);
    }
}
