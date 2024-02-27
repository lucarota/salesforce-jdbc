package com.ascendix.jdbc.salesforce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Properties;
import org.junit.Test;

public class ForceDriverTest {

    @Test
    public void testGetConnStringProperties_ListNoHost() throws IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://prop1=val1;prop2=val2");

        assertEquals(2, actual.size());
        assertEquals("val1", actual.getProperty("prop1"));
        assertEquals("val2", actual.getProperty("prop2"));
    }

    @Test
    public void testGetConnStringProperties_ListWithHost() throws IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://login.salesforce.ru:7642;prop1=val1;prop2=val2");

        assertEquals(3, actual.size());
        assertEquals("login.salesforce.ru:7642", actual.getProperty("loginDomain"));
        assertEquals("val1", actual.getProperty("prop1"));
        assertEquals("val2", actual.getProperty("prop2"));
    }

    @Test
    public void testGetConnStringProperties_WhenNoValue() throws IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://prop1=val1; prop2; prop3 = val3");

        assertEquals(3, actual.size());
        assertTrue(actual.containsKey("prop2"));
        assertEquals("", actual.getProperty("prop2"));
    }

    private String renderResultSet(ResultSet results) throws SQLException {
        StringBuilder out = new StringBuilder();

        int count = 0;
        int columnsCount = results.getMetaData().getColumnCount();

        // print header
        for(int i = 0; i < columnsCount; i++) {
            out.append(results.getMetaData().getColumnName(i+1)).append("\t");
        }
        out.append("\n");

        while(results.next()) {
            for(int i = 0; i < columnsCount; i++) {
                out.append(" " + results.getString(i+1)).append("\t");
            }
            out.append("\n");
            count++;
        }
        out.append("-----------------\n");
        out.append(count).append(" records\n");
        if (results.getWarnings() != null) {
            out.append("----------------- WARNINGS:\n");
            SQLWarning warning = results.getWarnings();
            while(warning != null) {
                out.append(warning.getMessage()).append("\n");
                warning = warning.getNextWarning();
            }
        }
        return out.toString();
    }

    @Test
    public void testGetConnStringProperties_StandartUrlFormat() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://test@test.ru:aaaa!aaa@login.salesforce.ru:7642");

        assertEquals(3, actual.size());
        assertTrue(actual.containsKey("user"));
        assertEquals("test@test.ru", actual.getProperty("user"));
        assertEquals("aaaa!aaa", actual.getProperty("password"));
        assertEquals("login.salesforce.ru:7642", actual.getProperty("loginDomain"));
    }

    @Test
    public void testGetConnStringProperties_JdbcUrlFormatNoUser() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://login.salesforce.ru:7642");

        assertEquals(1, actual.size());
        assertEquals("login.salesforce.ru:7642", actual.getProperty("loginDomain"));
    }

    @Test
    public void testGetConnStringProperties_HostName() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("login.salesforce.ru:7642");

        assertEquals(2, actual.size());
        assertEquals("login.salesforce.ru:7642", actual.getProperty("loginDomain"));
        assertEquals(true, ForceDriver.resolveBooleanProperty(actual, "https", true));
    }

    @Test
    public void testGetConnStringProperties_HostNameHttp() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("http://login.salesforce.ru:7642");

        assertEquals(2, actual.size());
        assertEquals("login.salesforce.ru:7642", actual.getProperty("loginDomain"));
        assertEquals(false, ForceDriver.resolveBooleanProperty(actual, "https", true));
    }

    @Test
    public void testGetConnStringProperties_IP() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("192.168.0.2:7642");

        assertEquals(2, actual.size());
        assertEquals("192.168.0.2:7642", actual.getProperty("loginDomain"));
        assertEquals(true, ForceDriver.resolveBooleanProperty(actual, "https", true));
    }

    @Test
    public void testGetConnStringProperties_StandartUrlFormatHttpsApi() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://test@test.ru:aaaa!aaa@login.salesforce.ru?https=false&api=48.0");

        assertEquals(5, actual.size());
        assertTrue(actual.containsKey("user"));
        assertEquals("test@test.ru", actual.getProperty("user"));
        assertEquals("aaaa!aaa", actual.getProperty("password"));
        assertEquals("login.salesforce.ru", actual.getProperty("loginDomain"));
        assertEquals("false", actual.getProperty("https"));
        assertEquals("48.0", actual.getProperty("api"));
    }

    @Test
    public void testGetConnStringProperties_ClientDecoder() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:ascendix:salesforce://test@test.ru:aaaa!aaa@login.salesforce.ru?https=false&client=SfdcInternalQA%2F%2e%2e%2e&api=48.0");

        assertEquals(6, actual.size());
        assertTrue(actual.containsKey("user"));
        assertEquals("test@test.ru", actual.getProperty("user"));
        assertEquals("aaaa!aaa", actual.getProperty("password"));
        assertEquals("login.salesforce.ru", actual.getProperty("loginDomain"));
        assertEquals("false", actual.getProperty("https"));
        assertEquals("48.0", actual.getProperty("api"));
        assertEquals("SfdcInternalQA/...", actual.getProperty("client"));
    }
}
