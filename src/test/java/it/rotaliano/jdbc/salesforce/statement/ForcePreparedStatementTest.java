package it.rotaliano.jdbc.salesforce.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.ws.ConnectionException;
import it.rotaliano.jdbc.salesforce.connection.ForceConnection;
import it.rotaliano.jdbc.salesforce.delegates.PartnerService;
import it.rotaliano.jdbc.salesforce.exceptions.SalesforceRuntimeException;
import it.rotaliano.jdbc.salesforce.statement.ForcePreparedStatement.CacheMode;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ForcePreparedStatementTest {

    private final ForceConnection connection;

    ForcePreparedStatementTest() {
        connection = mock(ForceConnection.class);
    }

    @Test
    public void testGetParamClass() {
        assertEquals(String.class, SoqlParameterConverter.getParamClass("test"));
        assertEquals(Long.class, SoqlParameterConverter.getParamClass(1L));
        assertEquals(Object.class, SoqlParameterConverter.getParamClass(new SimpleDateFormat()));
        assertNull(SoqlParameterConverter.getParamClass(null));
    }

    @Test
    public void testToSoqlStringParam() {
        assertEquals("'\\\\''", SoqlParameterConverter.toSoqlStringParam("'"));
        assertEquals("'\\\\'", SoqlParameterConverter.toSoqlStringParam("\\"));
        assertEquals("'\\\\';DELETE DATABASE \\\\a'", SoqlParameterConverter.toSoqlStringParam("';DELETE DATABASE \\a"));
    }

    @Test
    public void testConvertToSoqlParam() {
        assertEquals("123.45", SoqlParameterConverter.convertToSoqlParam(123.45));
        assertEquals("123.45", SoqlParameterConverter.convertToSoqlParam(123.45f));
        assertEquals("123", SoqlParameterConverter.convertToSoqlParam(123L));
        assertEquals("123.45", SoqlParameterConverter.convertToSoqlParam(new BigDecimal("123.45")));
        assertEquals("2017-03-06T12:34:56+00:00", SoqlParameterConverter.convertToSoqlParam(new GregorianCalendar(2017, 2, 6, 12, 34, 56).getTime()));
        assertEquals("'\\\\'test\\\\'\\\\'", SoqlParameterConverter.convertToSoqlParam("'test'\\"));
        assertEquals("NULL", SoqlParameterConverter.convertToSoqlParam(null));
    }

    @Test
    public void testAddParameter() throws SQLException {
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
    public void testSetParams() throws SQLException {
        ForcePreparedStatement statement = new ForcePreparedStatement(connection, "");
        String query = "SELECT Something FROM Anything WERE name = ? AND age > ?";
        statement.addParameter(1, "one");
        statement.addParameter(2, 123);

        String actual = statement.setParams(query);

        assertEquals("SELECT Something FROM Anything WERE name = 'one' AND age > 123", actual);
    }


    @Test
    public void testGetCacheMode() throws Exception {
        ForcePreparedStatement statement = new ForcePreparedStatement(connection, "");


        assertEquals(ForcePreparedStatement.CacheMode.SESSION, getCacheMode(statement, "CACHE SESSION select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.GLOBAL, getCacheMode(statement, " Cache global select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.NO_CACHE, getCacheMode(statement,"select name from Account"));
        assertEquals(ForcePreparedStatement.CacheMode.NO_CACHE, getCacheMode(statement," Cache unknown select name from Account"));
    }

    @Test
    public void removeCacheHints() throws Exception {
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

        java.lang.reflect.Field field = stClass.getDeclaredField("cacheMode");
        field.setAccessible(true);
        return (CacheMode) field.get(statement);
    }

    private static String getSqlAfterRemoveCacheHints(ForcePreparedStatement statement, String query) throws Exception {
        final Class<? extends ForcePreparedStatement> stClass = statement.getClass();
        final Method setCacheMode = stClass.getDeclaredMethod("setCacheMode", String.class);
        setCacheMode.setAccessible(true);
        setCacheMode.invoke(statement, query);

        java.lang.reflect.Field field = stClass.getDeclaredField("soqlQuery");
        field.setAccessible(true);
        return (String) field.get(statement);
    }

    @Test
    public void testQueryErrorMessageIncludesSalesforceErrorAndQuery() throws Exception {
        // Setup
        String testQuery = "SELECT Id, Name FROM Kafka_Job__c WHERE Ip_Addressing_Request__c != null";
        String salesforceError = "field 'Ip_Addressing_Request__c' can not be filtered in a query call";

        ForceConnection mockConnection = mock(ForceConnection.class);
        PartnerService mockPartnerService = mock(PartnerService.class);
        when(mockConnection.getPartnerService()).thenReturn(mockPartnerService);

        // Mock describeSObject to return minimal field metadata
        DescribeSObjectResult mockDescribe = mock(DescribeSObjectResult.class);
        com.sforce.soap.partner.Field idField = new com.sforce.soap.partner.Field();
        idField.setName("Id");
        idField.setType(com.sforce.soap.partner.FieldType.id);
        com.sforce.soap.partner.Field nameField = new com.sforce.soap.partner.Field();
        nameField.setName("Name");
        nameField.setType(com.sforce.soap.partner.FieldType.string);
        when(mockDescribe.getFields()).thenReturn(new com.sforce.soap.partner.Field[]{idField, nameField});
        when(mockPartnerService.describeSObject(anyString())).thenReturn(mockDescribe);

        // Mock PartnerService to throw ConnectionException on query
        ConnectionException connectionException = new ConnectionException(salesforceError);
        when(mockPartnerService.queryStart(anyString(), any())).thenThrow(connectionException);

        ForcePreparedStatement statement = new ForcePreparedStatement(mockConnection, testQuery);

        // Execute and verify
        ResultSet rs = statement.executeQuery();
        SalesforceRuntimeException exception = assertThrows(SalesforceRuntimeException.class, () -> {
            rs.next(); // This triggers the actual query execution
        });

        // Verify error message includes both Salesforce error and the query
        String errorMessage = exception.getMessage();
        assertTrue(errorMessage.contains(salesforceError),
            "Error message should contain Salesforce error: " + errorMessage);
        assertTrue(errorMessage.contains("Kafka_Job__c"),
            "Error message should contain the table name: " + errorMessage);
        assertTrue(errorMessage.contains("Query execution failed"),
            "Error message should contain 'Query execution failed': " + errorMessage);
    }
}
