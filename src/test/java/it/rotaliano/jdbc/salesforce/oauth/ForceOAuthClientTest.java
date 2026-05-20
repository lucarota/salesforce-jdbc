package it.rotaliano.jdbc.salesforce.oauth;

import it.rotaliano.jdbc.salesforce.connection.ForceConnectionInfo;
import it.rotaliano.jdbc.salesforce.connection.ForceService;
import it.rotaliano.jdbc.salesforce.connection.ForceSessionRenewal;
import it.rotaliano.jdbc.salesforce.exceptions.SalesforceRuntimeException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;

public class ForceOAuthClientTest {

    @Test
    public void testGetClientCredentialsToken_Success() throws Exception {
        HttpURLConnection mockConnection = Mockito.mock(HttpURLConnection.class);
        Mockito.when(mockConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
        
        String jsonResponse = "{\"access_token\":\"testToken123\",\"instance_url\":\"https://myinstance.salesforce.com\"}";
        ByteArrayInputStream is = new ByteArrayInputStream(jsonResponse.getBytes(StandardCharsets.UTF_8));
        Mockito.when(mockConnection.getInputStream()).thenReturn(is);
        
        java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
        Mockito.when(mockConnection.getOutputStream()).thenReturn(os);

        ForceOAuthClient client = new ForceOAuthClient(1000, 1000) {
            @Override
            protected HttpURLConnection openConnection(String spec) throws IOException {
                assertEquals("https://mycompany.my.salesforce.com/services/oauth2/token", spec);
                return mockConnection;
            }
        };

        ForceOAuthClient.TokenResponse res = client.getClientCredentialsToken("myid", "mysecret", "mycompany.my.salesforce.com");
        assertEquals("testToken123", res.getAccessToken());
        assertEquals("https://myinstance.salesforce.com", res.getInstanceUrl());
    }

    @Test
    public void testGetClientCredentialsToken_ErrorDetail() throws Exception {
        HttpURLConnection mockConnection = Mockito.mock(HttpURLConnection.class);
        Mockito.when(mockConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
        
        String errorJson = "{\"error\":\"invalid_client\",\"error_description\":\"bad client credentials\"}";
        ByteArrayInputStream errorStream = new ByteArrayInputStream(errorJson.getBytes(StandardCharsets.UTF_8));
        Mockito.when(mockConnection.getErrorStream()).thenReturn(errorStream);
        
        java.io.ByteArrayOutputStream os = new java.io.ByteArrayOutputStream();
        Mockito.when(mockConnection.getOutputStream()).thenReturn(os);

        ForceOAuthClient client = new ForceOAuthClient(1000, 1000) {
            @Override
            protected HttpURLConnection openConnection(String spec) {
                return mockConnection;
            }
        };

        ForceClientException exception = assertThrows(ForceClientException.class, () -> {
            client.getClientCredentialsToken("myid", "mysecret", "mycompany.my.salesforce.com");
        });

        assertTrue(exception.getMessage().contains("bad client credentials"));
    }

    @Test
    public void testGetUserInfoWithCustomDomain_Success() throws Exception {
        HttpURLConnection mockConnection = Mockito.mock(HttpURLConnection.class);
        Mockito.when(mockConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
        
        String userInfoJson = "{\n" +
                "   \"user_id\": \"12345\",\n" +
                "   \"organization_id\": \"67890\",\n" +
                "   \"urls\": {\n" +
                "      \"partner\": \"https://myinstance.salesforce.com/services/data/v{version}/sobjects/\"\n" +
                "   }\n" +
                "}";
        ByteArrayInputStream is = new ByteArrayInputStream(userInfoJson.getBytes(StandardCharsets.UTF_8));
        Mockito.when(mockConnection.getInputStream()).thenReturn(is);

        ForceOAuthClient client = new ForceOAuthClient(1000, 1000) {
            @Override
            protected HttpURLConnection openConnection(String spec) throws IOException {
                assertEquals("https://myinstance.salesforce.com/services/oauth2/userinfo", spec);
                return mockConnection;
            }
        };

        ForceUserInfo info = client.getUserInfoWithCustomDomain("testToken123", "https://myinstance.salesforce.com/");
        assertEquals("12345", info.getUserId());
        assertEquals("67890", info.getOrganizationId());
    }

    @Test
    public void testForceService_ValidationAndFlow() throws Exception {
        // Test generic login domain validation
        ForceConnectionInfo invalidInfo = new ForceConnectionInfo();
        invalidInfo.setClientId("client");
        invalidInfo.setClientSecret("secret");
        invalidInfo.setLoginDomain("login.salesforce.com");

        assertThrows(SalesforceRuntimeException.class, () -> {
            ForceService.createPartnerConnection(invalidInfo);
        });

        invalidInfo.setLoginDomain("test.salesforce.com");
        assertThrows(SalesforceRuntimeException.class, () -> {
            ForceService.createPartnerConnection(invalidInfo);
        });

        // Test successful creation of partner connection using client credentials
        ForceConnectionInfo info = new ForceConnectionInfo();
        info.setClientId("client");
        info.setClientSecret("secret");
        info.setLoginDomain("mycompany.my.salesforce.com");

        try (MockedConstruction<ForceOAuthClient> mocked = Mockito.mockConstruction(ForceOAuthClient.class,
                (mock, context) -> {
                    Mockito.when(mock.getClientCredentialsToken(anyString(), anyString(), anyString()))
                            .thenReturn(new ForceOAuthClient.TokenResponse("mockToken", "https://mycompany.my.salesforce.com"));
                    
                    ForceUserInfo userInfo = new ForceUserInfo();
                    userInfo.setUserId("123");
                    userInfo.setOrganizationId("00D000000000001");
                    userInfo.setPartnerUrl("https://mycompany.my.salesforce.com/services/Soap/u/64.0");
                    Mockito.when(mock.getUserInfoWithCustomDomain(anyString(), anyString()))
                            .thenReturn(userInfo);
                })) {

            try {
                ForceService.createPartnerConnection(info);
            } catch (Exception ignored) {
                // Expected ConnectionException since mock SOAP endpoint is not active
            }
            assertEquals("mockToken", info.getSessionId());
        }
    }

    @Test
    public void testForceSessionRenewal_RenewSession() throws Exception {
        ForceConnectionInfo info = new ForceConnectionInfo();
        info.setClientId("client");
        info.setClientSecret("secret");
        info.setLoginDomain("mycompany.my.salesforce.com");

        ForceSessionRenewal renewal = new ForceSessionRenewal(info);
        ConnectorConfig config = new ConnectorConfig();
        config.setSessionId("oldToken");

        try (MockedConstruction<ForceOAuthClient> mocked = Mockito.mockConstruction(ForceOAuthClient.class,
                (mock, context) -> {
                    Mockito.when(mock.getClientCredentialsToken(anyString(), anyString(), anyString()))
                            .thenReturn(new ForceOAuthClient.TokenResponse("newMockToken", "https://mycompany.my.salesforce.com"));
                    
                    ForceUserInfo userInfo = new ForceUserInfo();
                    userInfo.setUserId("123");
                    userInfo.setOrganizationId("00D000000000001");
                    userInfo.setPartnerUrl("https://mycompany.my.salesforce.com/services/Soap/u/64.0");
                    Mockito.when(mock.getUserInfoWithCustomDomain(anyString(), anyString()))
                            .thenReturn(userInfo);
                })) {

            SessionRenewer.SessionRenewalHeader header = renewal.renewSession(config);
            assertNotNull(header);
            assertEquals("newMockToken", config.getSessionId());
            assertEquals("newMockToken", info.getSessionId());
        }
    }
}
