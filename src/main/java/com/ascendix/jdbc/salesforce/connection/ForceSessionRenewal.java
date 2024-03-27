package com.ascendix.jdbc.salesforce.connection;

import com.ascendix.jdbc.salesforce.ForceDriver;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SessionHeader_element;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.SessionRenewer;

import javax.xml.namespace.QName;

public class ForceSessionRenewal implements SessionRenewer {
    @Override
    public SessionRenewalHeader renewSession(ConnectorConfig config) throws ConnectionException {
        config.setSessionId(null);
        PartnerConnection partnerConnection = Connector.newConnection(config);
        SessionHeader_element sessionHeader = partnerConnection.getSessionHeader();
        SessionRenewalHeader header = new SessionRenewalHeader();
        header.name = new QName("urn:partner.soap.sforce.com", "SessionHeader");
        header.headerElement = sessionHeader;
        if (sessionHeader != null && sessionHeader.getSessionId() != null) {
            ForceDriver.setSessionId(sessionHeader.getSessionId());
        }
        return header;
    }
}
