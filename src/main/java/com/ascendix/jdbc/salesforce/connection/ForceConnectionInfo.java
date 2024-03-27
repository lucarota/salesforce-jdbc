package com.ascendix.jdbc.salesforce.connection;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ForceConnectionInfo {

    private String userName;
    private String password;
    private String sessionId;
    private boolean sandbox;
    private boolean https = true;
    private boolean verifyConnectivity = false;
    private String apiVersion = ForceService.DEFAULT_API_VERSION;
    private String loginDomain;
    private String clientName;
    private String logfile;
    private int connectionTimeout = 10 * 1000;
    private int readTimeout = 30 * 1000;
}
