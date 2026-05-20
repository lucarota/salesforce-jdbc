package it.rotaliano.jdbc.salesforce.oauth;

import it.rotaliano.jdbc.salesforce.DriverConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

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
    private static final int MAX_RETRIES = DriverConfiguration.getMaxRetries();

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

    protected HttpURLConnection openConnection(String spec) throws IOException {
        return (HttpURLConnection) new URL(spec).openConnection();
    }

    private HttpURLConnection createConnection(String loginUrl, String accessToken) throws IOException {
        HttpURLConnection connection = openConnection(loginUrl);
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

    public static class TokenResponse {
        private final String accessToken;
        private final String instanceUrl;

        public TokenResponse(String accessToken, String instanceUrl) {
            this.accessToken = accessToken;
            this.instanceUrl = instanceUrl;
        }

        public String getAccessToken() {
            return accessToken;
        }

        public String getInstanceUrl() {
            return instanceUrl;
        }
    }

    public TokenResponse getClientCredentialsToken(String clientId, String clientSecret, String loginDomain) {
        String tokenUrl = "https://" + loginDomain + "/services/oauth2/token";
        int tryCount = 0;

        while (tryCount < MAX_RETRIES) {
            try {
                HttpURLConnection connection = openConnection(tokenUrl);
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
                connection.setRequestProperty("Accept", "application/json");
                connection.setDoOutput(true);
                connection.setConnectTimeout(Math.toIntExact(connectTimeout));
                connection.setReadTimeout(Math.toIntExact(readTimeout));

                String query = "grant_type=client_credentials" +
                        "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8.name()) +
                        "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8.name());

                try (java.io.OutputStream os = connection.getOutputStream()) {
                    os.write(query.getBytes(StandardCharsets.UTF_8));
                }

                int responseCode = connection.getResponseCode();
                if (responseCode == HttpURLConnection.HTTP_OK) {
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                        StringBuilder response = new StringBuilder();
                        String line;
                        while ((line = in.readLine()) != null) {
                            response.append(line);
                        }

                        JSONParser parser = new JSONParser(response.toString());
                        Object parsed = parser.parse();
                        if (parsed instanceof java.util.Map<?, ?> map) {
                            String accessToken = (String) map.get("access_token");
                            String instanceUrl = (String) map.get("instance_url");
                            if (StringUtils.isBlank(accessToken)) {
                                throw new ForceClientException("OAuth response is missing access_token");
                            }
                            return new TokenResponse(accessToken, instanceUrl);
                        } else {
                            throw new ForceClientException("OAuth response is not a JSON object");
                        }
                    } finally {
                        connection.disconnect();
                    }
                } else {
                    String errorDetail = "";
                    try (java.io.InputStream errorStream = connection.getErrorStream()) {
                        if (errorStream != null) {
                            try (BufferedReader in = new BufferedReader(new InputStreamReader(errorStream))) {
                                StringBuilder sb = new StringBuilder();
                                String line;
                                while ((line = in.readLine()) != null) {
                                    sb.append(line);
                                }
                                errorDetail = " Details: " + sb.toString();
                            }
                        }
                    } catch (Exception ignored) {}

                    if (responseCode >= 500 && responseCode < 600) {
                        tryCount++;
                    } else {
                        throw new ForceClientException("Client credentials token request failed with status: " + responseCode + errorDetail);
                    }
                }
            } catch (IOException e) {
                tryCount++;
                if (tryCount >= MAX_RETRIES) {
                    throw new ForceClientException("Failed to get token: Maximum retries exceeded", e);
                }
            }
        }
        throw new ForceClientException("Maximum retries exceeded");
    }

    public ForceUserInfo getUserInfoWithCustomDomain(String accessToken, String instanceUrl) {
        String baseUrl = instanceUrl;
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }
        String loginUrl = baseUrl + "/services/oauth2/userinfo";
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
                    tryCount++;
                } else {
                    throw new ForceClientException("Response error: " + responseCode + " " + connection.getResponseMessage());
                }
            } catch (IOException e) {
                tryCount++;
            }
        }

        throw new ForceClientException("Maximum retries exceeded");
    }
}
