package com.ascendix.jdbc.salesforce.oauth;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

@Slf4j
public class ForceOAuthClient {

    private static final String LOGIN_URL = "https://login.salesforce.com/services/oauth2/userinfo";
    private static final String TEST_LOGIN_URL = "https://test.salesforce.com/services/oauth2/userinfo";

    private static final String BAD_TOKEN_SF_ERROR_CODE = "Bad_OAuth_Token";
    private static final String MISSING_TOKEN_SF_ERROR_CODE = "Missing_OAuth_Token";
    private static final String WRONG_ORG_SF_ERROR_CODE = "Wrong_Org";
    private static final String FORBIDDEN = "Forbidden";
    private static final String BAD_ID_SF_ERROR_CODE = "Bad_Id";
    private static final String INTERNAL_SERVER_ERROR_SF_ERROR_CODE = "Internal Error";
    private static final int MAX_RETRIES = 5;

    private final long connectTimeout;
    private final long readTimeout;

    public ForceOAuthClient(long connectTimeout, long readTimeout) {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    public ForceUserInfo getUserInfo(String accessToken, boolean sandbox) {
        String loginUrl = sandbox ? TEST_LOGIN_URL : LOGIN_URL;
        int tryCount = 0;

        while (tryCount < MAX_RETRIES) {
            try {
                HttpURLConnection connection = createConnection(loginUrl, accessToken);
                int responseCode = connection.getResponseCode();

                if (responseCode == HttpURLConnection.HTTP_OK) {
                    return parseResponse(connection);
                } else if (isBadTokenError(responseCode, connection.getResponseMessage())) {
                    throw new BadOAuthTokenException("Bad OAuth Token: " + accessToken);
                } else if (isForceInternalError(responseCode, connection.getResponseMessage())) {
                    tryCount++; // Retry
                } else {
                    throw new ForceClientException("Response error: " + responseCode + " " + connection.getResponseMessage());
                }
            } catch (IOException e) {
                tryCount++;
            }
        }

        throw new ForceClientException("Maximum retries exceeded");
    }

    private HttpURLConnection createConnection(String loginUrl, String accessToken) throws IOException {
        URL url = new URL(loginUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestProperty("Authorization", "Bearer " + accessToken);
        connection.setDoInput(true);
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(Math.toIntExact(connectTimeout));
        connection.setReadTimeout(Math.toIntExact(readTimeout));
        return connection;
    }

    private ForceUserInfo parseResponse(HttpURLConnection connection) throws IOException {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            return ForceUserInfoParser.parse(response.toString());
        } finally {
            connection.disconnect();
        }
    }

    private boolean isBadTokenError(int statusCode, String content) {
        return ((statusCode == HttpURLConnection.HTTP_FORBIDDEN) &&
            StringUtils.equalsAnyIgnoreCase(content,
                BAD_TOKEN_SF_ERROR_CODE, MISSING_TOKEN_SF_ERROR_CODE,
                WRONG_ORG_SF_ERROR_CODE, FORBIDDEN)) ||
            (statusCode == HttpURLConnection.HTTP_NOT_FOUND && StringUtils.equalsIgnoreCase(content, BAD_ID_SF_ERROR_CODE));
    }

    private boolean isForceInternalError(int statusCode, String content) {
        return statusCode == HttpURLConnection.HTTP_NOT_FOUND && StringUtils.equalsIgnoreCase(content,
                INTERNAL_SERVER_ERROR_SF_ERROR_CODE);
    }
}
