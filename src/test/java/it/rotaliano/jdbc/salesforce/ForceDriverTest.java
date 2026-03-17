package it.rotaliano.jdbc.salesforce;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ForceDriverTest {

    @Test
    void testGetConnStringProperties_ListNoHost() throws IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://prop1=val1;prop2=val2");

        assertEquals(2, actual.size());
        assertEquals("val1", actual.getProperty("prop1"));
        assertEquals("val2", actual.getProperty("prop2"));
    }

    @Test
    void testGetConnStringProperties_ListWithHost() throws IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://login.salesforce.com;prop1=val1;prop2=val2");

        assertEquals(3, actual.size());
        assertEquals("login.salesforce.com", actual.getProperty("loginDomain"));
        assertEquals("val1", actual.getProperty("prop1"));
        assertEquals("val2", actual.getProperty("prop2"));
    }

    @Test
    void testGetConnStringProperties_WhenNoValue() throws IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://prop1=val1; prop2; prop3 = val3");

        assertEquals(3, actual.size());
        assertTrue(actual.containsKey("prop2"));
        assertEquals("", actual.getProperty("prop2"));
    }

    @Test
    void testGetConnStringProperties_StandartUrlFormat() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://test@test.ru:aaaa!aaa@login.salesforce.com");

        assertEquals(3, actual.size());
        assertTrue(actual.containsKey("user"));
        assertEquals("test@test.ru", actual.getProperty("user"));
        assertEquals("aaaa!aaa", actual.getProperty("password"));
        assertEquals("login.salesforce.com", actual.getProperty("loginDomain"));
    }

    @Test
    void testGetConnStringProperties_JdbcUrlFormatNoUser() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://login.salesforce.com");

        assertEquals(1, actual.size());
        assertEquals("login.salesforce.com", actual.getProperty("loginDomain"));
    }

    @Test
    void testGetConnStringProperties_HostName() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("login.salesforce.com");

        assertEquals(2, actual.size());
        assertEquals("login.salesforce.com", actual.getProperty("loginDomain"));
        assertEquals("true", actual.getProperty("https"));
    }

    @Test
    void testGetConnStringProperties_HostNameHttp() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("http://login.salesforce.com");

        assertEquals(2, actual.size());
        assertEquals("login.salesforce.com", actual.getProperty("loginDomain"));
        assertEquals("false", actual.getProperty("https"));
    }

    @Test
    void testGetConnStringProperties_IP() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("192.168.0.2:7642");

        assertEquals(2, actual.size());
        assertEquals("192.168.0.2:7642", actual.getProperty("loginDomain"));
        assertEquals("true", actual.getProperty("https"));
    }

    @Test
    void testGetConnStringProperties_StandardUrlFormatHttpsApi() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://test@test.ru:aaaa!aaa@login.salesforce.ru?https=false&api=48.0");

        assertEquals(5, actual.size());
        assertTrue(actual.containsKey("user"));
        assertEquals("test@test.ru", actual.getProperty("user"));
        assertEquals("aaaa!aaa", actual.getProperty("password"));
        assertEquals("login.salesforce.ru", actual.getProperty("loginDomain"));
        assertEquals("false", actual.getProperty("https"));
        assertEquals("48.0", actual.getProperty("api"));
    }

    @Test
    void testGetConnStringProperties_ClientDecoder() throws  IOException {
        Properties actual = ForceDriver.getConnStringProperties("jdbc:rotaliano:salesforce://test@test.ru:aaaa!aaa@login.salesforce.ru?https=false&client=SfdcInternalQA%2F%2e%2e%2e&api=48.0");

        assertEquals(6, actual.size());
        assertTrue(actual.containsKey("user"));
        assertEquals("test@test.ru", actual.getProperty("user"));
        assertEquals("aaaa!aaa", actual.getProperty("password"));
        assertEquals("login.salesforce.ru", actual.getProperty("loginDomain"));
        assertEquals("false", actual.getProperty("https"));
        assertEquals("48.0", actual.getProperty("api"));
        assertEquals("SfdcInternalQA/...", actual.getProperty("client"));
    }

    @Nested
    class SanitizeUrlTests {

        @Test
        void testSanitizeUrl_withCredentials() {
            String url = "jdbc:rotaliano:salesforce://admin:MyS3cretPass@login.salesforce.com";
            String sanitized = ForceDriver.sanitizeUrl(url);
            assertEquals("jdbc:rotaliano:salesforce://admin:****@login.salesforce.com", sanitized);
        }

        @Test
        void testSanitizeUrl_withCredentialsAndParams() {
            String url = "jdbc:rotaliano:salesforce://admin:password123@host.sf.com?https=false&api=64";
            String sanitized = ForceDriver.sanitizeUrl(url);
            assertEquals("jdbc:rotaliano:salesforce://admin:****@host.sf.com?https=false&api=64", sanitized);
        }

        @Test
        void testSanitizeUrl_withoutCredentials() {
            String url = "jdbc:rotaliano:salesforce://login.salesforce.com;user=admin;password=secret";
            String sanitized = ForceDriver.sanitizeUrl(url);
            assertEquals(url, sanitized);
        }

        @Test
        void testSanitizeUrl_nullUrl() {
            assertNull(ForceDriver.sanitizeUrl(null));
        }
    }
}
