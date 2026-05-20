package it.rotaliano.jdbc.salesforce.connection;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SessionHeader_element;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;
import it.rotaliano.jdbc.salesforce.oauth.ForceOAuthClient;
import it.rotaliano.jdbc.salesforce.oauth.ForceUserInfo;
import org.apache.commons.lang3.StringUtils;

import javax.xml.namespace.QName;

public class ForceSessionRenewal implements SessionRenewer {
    private final ForceConnectionInfo connectionInfo;

    public ForceSessionRenewal() {
        this.connectionInfo = null;
    }

    public ForceSessionRenewal(ForceConnectionInfo connectionInfo) {
        this.connectionInfo = connectionInfo;
    }

    @Override
    public SessionRenewalHeader renewSession(ConnectorConfig config) throws ConnectionException {
        if (connectionInfo != null &&
                StringUtils.isNotBlank(connectionInfo.getClientId()) &&
                StringUtils.isNotBlank(connectionInfo.getClientSecret())) {
            try {
                ForceOAuthClient oauthClient = new ForceOAuthClient(
                        connectionInfo.getConnectionTimeout(), connectionInfo.getReadTimeout()
                );
                ForceOAuthClient.TokenResponse tokenRes = oauthClient.getClientCredentialsToken(
                        connectionInfo.getClientId(), connectionInfo.getClientSecret(),
                        connectionInfo.getLoginDomain()
                );
                
                String newAccessToken = tokenRes.getAccessToken();
                connectionInfo.setSessionId(newAccessToken);
                config.setSessionId(newAccessToken);

                // Update the user info cache with the new token
                ForceUserInfo userInfo = oauthClient.getUserInfoWithCustomDomain(newAccessToken, tokenRes.getInstanceUrl());
                ForceService.cacheUserInfo(newAccessToken, userInfo);

                SessionHeader_element sessionHeader = new SessionHeader_element();
                sessionHeader.setSessionId(newAccessToken);

                SessionRenewalHeader header = new SessionRenewalHeader();
                header.name = new QName("urn:partner.soap.sforce.com", "SessionHeader");
                header.headerElement = sessionHeader;
                return header;
            } catch (Exception e) {
                throw new ConnectionException("Failed to renew session via OAuth Client Credentials", e);
            }
        } else {
            config.setSessionId(null);
            PartnerConnection partnerConnection = Connector.newConnection(config);
            SessionHeader_element sessionHeader = partnerConnection.getSessionHeader();
            SessionRenewalHeader header = new SessionRenewalHeader();
            header.name = new QName("urn:partner.soap.sforce.com", "SessionHeader");
            header.headerElement = sessionHeader;
            if (sessionHeader != null && sessionHeader.getSessionId() != null) {
                config.setSessionId(sessionHeader.getSessionId());
                if (connectionInfo != null) {
                    connectionInfo.setSessionId(sessionHeader.getSessionId());
                }
            }
            return header;
        }
    }
}
