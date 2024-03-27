package com.ascendix.jdbc.salesforce.soap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ForceSoapValidator {

    private static final String SOAP_ACTION = "some";
    private static final String SOAP_FAULT = "<soapenv:Fault>";
    private static final String BAD_TOKEN_SF_ERROR_CODE = "INVALID_SESSION_ID";

    private final long connectTimeout;
    private final long readTimeout;

    public ForceSoapValidator(long connectTimeout, long readTimeout) {
        this.connectTimeout = connectTimeout;
        this.readTimeout = readTimeout;
    }

    public boolean validateForceToken(String partnerUrl, String accessToken) {
        StringBuilder requestBody = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(getClass().getResourceAsStream("/forceSoapBody"), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                requestBody.append(line).append("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading SOAP body", e);
        }

        String soapBody = requestBody.toString().replace("{sessionId}", accessToken);
        byte[] encodedSoapBody = Base64.getEncoder().encode(soapBody.getBytes(StandardCharsets.UTF_8));

        try {
            URL url = new URL(partnerUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "text/xml;charset=UTF-8");
            connection.setRequestProperty("SOAPAction", SOAP_ACTION);
            connection.setDoOutput(true);
            connection.setConnectTimeout((int) connectTimeout);
            connection.setReadTimeout((int) readTimeout);
            connection.getOutputStream().write(encodedSoapBody);

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                return true;
            } else if (responseCode == HttpURLConnection.HTTP_INTERNAL_ERROR) {
                String errorMessage = new String(connection.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                return (containsIgnoreCase(errorMessage, SOAP_FAULT) &&
                    containsIgnoreCase(errorMessage, BAD_TOKEN_SF_ERROR_CODE));
            } else {
                throw new RuntimeException("Response error: " + responseCode);
            }
        } catch (IOException e) {
            throw new RuntimeException("IO error: " + e.getMessage(), e);
        }
    }

    private boolean containsIgnoreCase(String input, String searchString) {
        return input.toUpperCase().contains(searchString.toUpperCase());
    }

}