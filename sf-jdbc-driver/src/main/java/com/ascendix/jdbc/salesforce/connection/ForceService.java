package com.ascendix.jdbc.salesforce.connection;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.ascendix.jdbc.salesforce.transport.HttpClientTransport;
import com.ascendix.salesforce.oauth.ForceOAuthClient;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.fault.UnexpectedErrorFault;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.experimental.UtilityClass;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

@UtilityClass
public class ForceService {

    private static final Logger logger = Logger.getLogger(ForceDriver.SF_JDBC_DRIVER_NAME);

    public static final String DEFAULT_LOGIN_DOMAIN = "login.salesforce.com";
    private static final String SANDBOX_LOGIN_DOMAIN = "test.salesforce.com";
    private static final long OAUTH_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(10);
    private static final long OAUTH_READ_TIMEOUT = TimeUnit.SECONDS.toMillis(30);
    public static final String DEFAULT_API_VERSION = "50.0";
    public static final int EXPIRE_AFTER_CREATE = 60;
    public static final int EXPIRE_STORE_SIZE = 16;

    private static final DB cacheDb = DBMaker.tempFileDB().closeOnJvmShutdown().make();

    private static final HTreeMap<String, String> partnerUrlCache = cacheDb
        .hashMap("PartnerUrlCache", Serializer.STRING, Serializer.STRING)
        .expireAfterCreate(EXPIRE_AFTER_CREATE, TimeUnit.MINUTES)
        .expireStoreSize(EXPIRE_STORE_SIZE * FileUtils.ONE_MB)
        .create();

    private static String getPartnerUrl(String accessToken, boolean sandbox) {
        return partnerUrlCache.computeIfAbsent(accessToken, s -> getPartnerUrlFromUserInfo(accessToken, sandbox));
    }

    private static String getPartnerUrlFromUserInfo(String accessToken, boolean sandbox) {
        return new ForceOAuthClient(OAUTH_CONNECTION_TIMEOUT, OAUTH_READ_TIMEOUT).getUserInfo(accessToken, sandbox).getPartnerUrl();
    }

    public static PartnerConnection createPartnerConnection(ForceConnectionInfo info) throws ConnectionException {
        return info.getSessionId() != null ? createConnectionBySessionId(info) : createConnectionByUserCredential(info);
    }

    private static PartnerConnection createConnectionBySessionId(ForceConnectionInfo info) throws ConnectionException {
        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setReadTimeout(info.getReadTimeout());
        partnerConfig.setConnectionTimeout(info.getConnectionTimeout());
        partnerConfig.setSessionId(info.getSessionId());
        if (info.getLogfile() != null) {
            try {
                partnerConfig.setTraceFile(info.getLogfile());
                partnerConfig.setTraceMessage(true);
            } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, "Error creating log file", e);
            }
        }

        if (info.getSandbox() != null) {
            partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(info.getSessionId(), info.getSandbox()));
            return Connector.newConnection(partnerConfig);
        }

        try {
            partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(info.getSessionId(), false));
            return Connector.newConnection(partnerConfig);
        } catch (RuntimeException re) {
            try {
                partnerConfig.setServiceEndpoint(ForceService.getPartnerUrl(info.getSessionId(), true));
                return Connector.newConnection(partnerConfig);
            } catch (RuntimeException r) {
                throw new ConnectionException(r.getMessage());
            }
        }
    }

    private static PartnerConnection createConnectionByUserCredential(ForceConnectionInfo info)
        throws ConnectionException {

        ConnectorConfig partnerConfig = new ConnectorConfig();
        partnerConfig.setUsername(info.getUserName());
        partnerConfig.setPassword(info.getPassword());
        partnerConfig.setReadTimeout(info.getReadTimeout());
        partnerConfig.setConnectionTimeout(info.getConnectionTimeout());
        partnerConfig.setTransport(HttpClientTransport.class);
        if (info.getLogfile() != null) {
            try {
                partnerConfig.setTraceFile(info.getLogfile());
                partnerConfig.setTraceMessage(true);
            } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, "Error creating log file", e);
            }
        }

        PartnerConnection connection;

        if (info.getSandbox() != null) {
            partnerConfig.setAuthEndpoint(buildAuthEndpoint(info));
            connection = Connector.newConnection(partnerConfig);
        } else {
            try {
                info.setSandbox(false);
                partnerConfig.setAuthEndpoint(buildAuthEndpoint(info));
                connection = Connector.newConnection(partnerConfig);
            } catch (ConnectionException ce) {
                info.setSandbox(true);
                partnerConfig.setAuthEndpoint(buildAuthEndpoint(info));
                connection = Connector.newConnection(partnerConfig);
            }
        }
        if (StringUtils.isNotBlank(info.getClientName())) {
            connection.setCallOptions(info.getClientName(), null);
        }
        verifyConnectivity(connection);
        return connection;
    }

    private static void verifyConnectivity(PartnerConnection connection) throws ConnectionException {
        try {
            QueryResult select_name_from_organization = connection.query("Select Name from Organization");
            if (select_name_from_organization.getSize() != 1) {
                logger.log(Level.SEVERE, "Unable to verify connectivity for URL provided");
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
                    logger.log(Level.SEVERE, "The format of the URL is wrong", ex);
                    throw new ConnectionException("Failed to connect to the host", ex);
                } catch (IOException ioException) {
                    logger.log(Level.SEVERE, "Failed to connect to the host", ioException);
                    throw new ConnectionException("Failed to connect to the host", ioException);
                }
            }
        } catch (ConnectionException e) {
            logger.log(Level.SEVERE, "Failed to establish connection", e);
            throw new ConnectionException("Failed to connect, unexpected error:", e);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to connect, unexpected error:", e);
            throw new ConnectionException("Failed to connect, unexpected error:", e);
        }
    }

    private static String buildAuthEndpoint(ForceConnectionInfo info) {
        String protocol = info.getHttps() ? "https" : "http";
        String domain = info.getSandbox() ? SANDBOX_LOGIN_DOMAIN
            : info.getLoginDomain() != null ? info.getLoginDomain() : DEFAULT_LOGIN_DOMAIN;
        return String.format("%s://%s/services/Soap/u/%s", protocol, domain, info.getApiVersion());
    }
}
