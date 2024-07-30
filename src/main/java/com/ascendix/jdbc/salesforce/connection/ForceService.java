package com.ascendix.jdbc.salesforce.connection;

import com.ascendix.jdbc.salesforce.oauth.BadOAuthTokenException;
import com.ascendix.jdbc.salesforce.oauth.ForceOAuthClient;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SessionHeader_element;
import com.sforce.soap.partner.fault.UnexpectedErrorFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@UtilityClass
@Slf4j
public class ForceService {

    public static final String DEFAULT_LOGIN_DOMAIN = "login.salesforce.com";
    private static final String SANDBOX_LOGIN_DOMAIN = "test.salesforce.com";
    private static final long OAUTH_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    private static final long OAUTH_READ_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    public static final String DEFAULT_API_VERSION = "61";

    private static final Map<String, String> partnerUrlCache = new HashMap<>();
    public static final String UNEXPECTED_ERROR = "Failed to connect, unexpected error:";
    public static final String CONNECT_FAILED = "Failed to connect to the host";

    private static String getPartnerUrl(String accessToken, boolean sandbox) {
        return partnerUrlCache.computeIfAbsent(accessToken, s -> getPartnerUrlFromUserInfo(accessToken, sandbox));
    }

    private static String getPartnerUrlFromUserInfo(String accessToken, boolean sandbox) {
        return new ForceOAuthClient(OAUTH_CONNECTION_TIMEOUT, OAUTH_READ_TIMEOUT).getUserInfo(accessToken, sandbox)
            .getPartnerUrl();
    }

    public static PartnerConnection createPartnerConnection(ForceConnectionInfo info) throws ConnectionException {
        PartnerConnection partnerConnection = createConnection(info);
        SessionHeader_element sessionHeader = partnerConnection.getSessionHeader();
        if (sessionHeader != null && sessionHeader.getSessionId() != null) {
            info.setSessionId(sessionHeader.getSessionId());
        }
        return partnerConnection;
    }

    private static PartnerConnection createConnection(ForceConnectionInfo info) throws ConnectionException {
        ConnectorConfig partnerConfig = convertForceConnectionInfo(info);
        final String sessionId = info.getSessionId();

        PartnerConnection connection;
        try {
            connection = Connector.newConnection(partnerConfig);
        } catch (ConnectionException ce) {
            if (!info.isSandbox()) {
                if (sessionId != null) {
                    partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(sessionId, true));
                    return Connector.newConnection(partnerConfig);
                } else {
                    info.setSandbox(true);
                    String endpoint = buildAuthEndpoint(info);
                    partnerConfig.setAuthEndpoint(endpoint);
                    partnerConfig.setServiceEndpoint(endpoint);
                    connection = Connector.newConnection(partnerConfig);
                }
            } else {
                throw ce;
            }
        }

        if (sessionId == null) {
            if (StringUtils.isNotBlank(info.getClientName())) {
                connection.setCallOptions(info.getClientName(), null);
            }
            if (info.isVerifyConnectivity()) {
                verifyConnectivity(connection);
            }
        }
        return connection;
    }

    private static ConnectorConfig convertForceConnectionInfo(ForceConnectionInfo info) {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setReadTimeout(info.getReadTimeout());
        partnerConfig.setConnectionTimeout(info.getConnectionTimeout());
        partnerConfig.setSessionRenewer(new ForceSessionRenewal());
        partnerConfig.setUsername(info.getUserName());
        partnerConfig.setPassword(info.getPassword());
        String sessionId = info.getSessionId();
        partnerConfig.setAuthEndpoint(buildAuthEndpoint(info));
        if (sessionId != null) {
            try {
                partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(sessionId, info.isSandbox()));
            } catch (BadOAuthTokenException e) {
                info.setSessionId(null);
                sessionId = null;
            }
        }
        partnerConfig.setSessionId(sessionId);

        if (info.getLogfile() != null) {
            try {
                partnerConfig.setTraceFile(info.getLogfile());
                partnerConfig.setTraceMessage(true);
            } catch (FileNotFoundException e) {
                log.warn("Error creating log file", e);
            }
        }

        return partnerConfig;
    }

    private static void verifyConnectivity(PartnerConnection connection) throws ConnectionException {
        try {
            QueryResult orgName = connection.query("Select Name from Organization");
            if (orgName.getSize() != 1) {
                log.error("Unable to verify connectivity for URL provided");
            }
        } catch (UnexpectedErrorFault e) {
            if (e.getExceptionCode() != null && "INVALID_SESSION_ID".equals(e.getExceptionCode().name())) {
                // Original EndPoint    : connection.getConfig().getAuthEndpoint()
                // Real Service EndPoint: connection.getConfig().getServiceEndpoint()
                try {
                    URL serviceUrl = new URL(connection.getConfig().getServiceEndpoint());
                    URLConnection urlConnection = serviceUrl.openConnection();
                    urlConnection.connect();
                } catch (MalformedURLException ex) {
                    log.error("The format of the URL is wrong", ex);
                    throw new ConnectionException(CONNECT_FAILED, ex);
                } catch (IOException ioException) {
                    log.error(CONNECT_FAILED, ioException);
                    throw new ConnectionException(CONNECT_FAILED, ioException);
                }
            }
        } catch (ConnectionException e) {
            log.error("Failed to establish connection", e);
            throw new ConnectionException(UNEXPECTED_ERROR, e);
        } catch (Exception e) {
            log.error(UNEXPECTED_ERROR, e);
            throw new ConnectionException(UNEXPECTED_ERROR, e);
        }
    }

    private static String buildAuthEndpoint(ForceConnectionInfo info) {
        String protocol = info.isHttps() ? "https" : "http";
        final String loginDomain = info.getLoginDomain() != null ? info.getLoginDomain() : DEFAULT_LOGIN_DOMAIN;
        String domain = info.isSandbox() ? SANDBOX_LOGIN_DOMAIN : loginDomain;
        return String.format("%s://%s/services/Soap/u/%s", protocol, domain, info.getApiVersion());
    }
}
