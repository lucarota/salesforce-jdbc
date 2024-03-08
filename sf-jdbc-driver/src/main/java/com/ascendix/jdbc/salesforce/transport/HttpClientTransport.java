/*
 * Copyright (c) 2015, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.ascendix.jdbc.salesforce.transport;

import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.tools.VersionInfo;
import com.sforce.ws.transport.LimitingOutputStream;
import com.sforce.ws.transport.MessageHandlerOutputStream;
import com.sforce.ws.transport.Transport;
import com.sforce.ws.util.FileUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.Credentials;
import org.apache.hc.client5.http.auth.NTCredentials;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.classic.methods.HttpHead;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;

/**
 * This class implements the Transport interface for WSC with HttpClient in order to properly work
 * with NTLM proxies.  The existing JdkHttpTransport in WSC does not work with NTLM proxies when
 * compiled on Java 1.6
 *
 * @author Jeff Lai
 * @since 25.0.2
 */
public class HttpClientTransport implements Transport {

    private static ConnectorConfig currentConfig = null;
    private boolean successful;
    private HttpUriRequestBase httpMethod;
    private ByteArrayOutputStream entityByteOut;
    private static CloseableHttpClient currentHttpClient = null;
    @Setter
    @Getter
    private static boolean reuseConnection = true;
    @Getter
    private static long serverInvocationCount = 0;

    public HttpClientTransport() {
    }

    public HttpClientTransport(ConnectorConfig newConfig) {
        setConfig(newConfig);
    }

    @Override
    public synchronized void setConfig(ConnectorConfig newConfig) {
        if (!areEquivalentConfigs(currentConfig, newConfig) && currentHttpClient != null) {
            try {
                currentHttpClient.close();
            } catch (IOException ex) {
                // do nothing
            }
            currentHttpClient = null;
        }
        currentConfig = newConfig;
    }

    @Override
    public OutputStream connect(String url, String soapAction) throws IOException {
        if (soapAction == null) {
            soapAction = "";
        }

        HashMap<String, String> header = new HashMap<String, String>();

        header.put("SOAPAction", "\"" + soapAction + "\"");
        header.put("Content-Type", "text/xml; charset=" + StandardCharsets.UTF_8.name());
        header.put("Accept", "text/xml");

        return connect(url, header);
    }

    private boolean areEquivalentConfigs(ConnectorConfig config1, ConnectorConfig config2) {
        if (config1 == null && config2 == null) {
            return true;
        } else if (config1 == null || config2 == null) {
            // one of the configs is null, other isn't. They can't be equal.
            return false;
        } else if (config1.equals(config2)) {
            return true;
        }

        InetSocketAddress socketAddress1 = (InetSocketAddress) config1.getProxy().address();
        InetSocketAddress socketAddress2 = (InetSocketAddress) config2.getProxy().address();

        if (socketAddress1 == null && socketAddress2 == null) {
            return true;
        } else if (socketAddress1 == null || socketAddress2 == null) {
            return false;
        } else {
            String field1, field2;
            field1 = config1.getProxyUsername() == null ? "" : config1.getProxyUsername();
            field2 = config2.getProxyUsername() == null ? "" : config2.getProxyUsername();
            if (field1.compareTo(field2) != 0) {
                return false;
            }

            field1 = config1.getProxyPassword() == null ? "" : config1.getProxyPassword();
            field2 = config2.getProxyPassword() == null ? "" : config2.getProxyPassword();
            if (field1.compareTo(field2) != 0) {
                return false;
            }

            field1 = config1.getNtlmDomain() == null ? "" : config1.getNtlmDomain();
            field2 = config2.getNtlmDomain() == null ? "" : config2.getNtlmDomain();
            if (field1.compareTo(field2) != 0) {
                return false;
            }

            field1 = socketAddress1.getHostName() == null ? "" : socketAddress1.getHostName();
            field2 = socketAddress2.getHostName() == null ? "" : socketAddress2.getHostName();
            if (field1.compareTo(field2) != 0) {
                return false;
            }

            int intField1 = socketAddress1.getPort();
            int intField2 = socketAddress2.getPort();
            return intField1 == intField2;
        }
    }

    private static synchronized void initializeHttpClient() throws UnknownHostException {
        if (!isReuseConnection()) {
            closeConnections();
            currentHttpClient = null;
        }
        if (currentHttpClient == null) {
            HttpClientBuilder httpClientBuilder = HttpClientBuilder.create().useSystemProperties();

            if (currentConfig.getProxy().address() != null) {
                String proxyUser = currentConfig.getProxyUsername() == null ? "" : currentConfig.getProxyUsername();
                char[] proxyPassword = currentConfig.getProxyPassword() == null ? new char[]{}
                    : currentConfig.getProxyPassword().toCharArray();

                Credentials credentials;

                if (currentConfig.getNtlmDomain() != null && !currentConfig.getNtlmDomain().isEmpty()) {
                    String computerName = InetAddress.getLocalHost().getCanonicalHostName();
                    credentials = new NTCredentials(proxyUser,
                        proxyPassword,
                        computerName,
                        currentConfig.getNtlmDomain());
                } else {
                    credentials = new UsernamePasswordCredentials(proxyUser, proxyPassword);
                }

                InetSocketAddress proxyAddress = (InetSocketAddress) currentConfig.getProxy().address();
                HttpHost proxyHost = new HttpHost("http", proxyAddress.getHostName(), proxyAddress.getPort());
                httpClientBuilder.setProxy(proxyHost);

                BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                AuthScope scope = new AuthScope(null, proxyAddress.getHostName(), proxyAddress.getPort(), null, null);
                credentialsProvider.setCredentials(scope, credentials);
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }

            currentHttpClient = httpClientBuilder.build();
        }
    }

    @Override
    public synchronized InputStream getContent() throws IOException {
        serverInvocationCount++;
        initializeHttpClient();
        if (this.httpMethod.getEntity() == null) {
            byte[] entityBytes = entityByteOut.toByteArray();
            HttpEntity entity = new ByteArrayEntity(entityBytes, ContentType.APPLICATION_XML);
            currentConfig.setUseChunkedPost(false);
            this.httpMethod.setEntity(entity);
        }
        InputStream input;
        try {
            HttpClientContext context = HttpClientContext.create();
            RequestConfig config = RequestConfig.custom()
                .setExpectContinueEnabled(currentConfig.useChunkedPost())
                .build();
            context.setRequestConfig(config);

            if (currentConfig.getNtlmDomain() != null && !currentConfig.getNtlmDomain().isEmpty()) {
                // need to send a HEAD request to trigger NTLM authentication
                try (CloseableHttpResponse ignored = currentHttpClient.execute(new HttpHead("http://salesforce.com"))) {
                }
            }

            try (CloseableHttpResponse response = currentHttpClient.execute(this.httpMethod, context)) {
                successful = true;
                if (response.getCode() > 399) {
                    successful = false;
                    if (response.getCode() == 407) {
                        throw new RuntimeException(
                            response.getCode() + " " + response.getReasonPhrase());
                    }
                }
                // copy input stream data into a new input stream because releasing the connection will close the input stream
                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                try (InputStream inStream = response.getEntity().getContent()) {
                    IOUtils.copy(inStream, bOut);
                    input = new ByteArrayInputStream(bOut.toByteArray());
                    if (response.containsHeader("Content-Encoding")
                        && response.getHeaders("Content-Encoding")[0].getValue().equals("gzip")) {
                        input = new GZIPInputStream(input);
                    }
                }

                if (currentConfig.isTraceMessage()) {
                    final String sb = String.valueOf(response.getVersion()) + ' ' + response.getCode() + ' ' + response.getReasonPhrase() + '\n';
                    currentConfig.getTraceStream().print(sb);
                    byte[] bytes = FileUtil.toBytes(input);
                    input = new ByteArrayInputStream(bytes);

                    Header[] headers = response.getHeaders();
                    for (Header header : headers) {
                        currentConfig.getTraceStream().print(header.getName());
                        currentConfig.getTraceStream().print("=");
                        currentConfig.getTraceStream().println(header.getValue());
                    }

                    currentConfig.teeInputStream(bytes);
                }
            }
        } finally {
            if (isReuseConnection()) {
                closeConnections();
            }
        }

        return input;
    }

    @Override
    public boolean isSuccessful() {
        return successful;
    }

    @Override
    public OutputStream connect(String endpoint, HashMap<String, String> httpHeaders) throws IOException {
        return connect(endpoint, httpHeaders, true);
    }

    @Override
    public OutputStream connect(String endpoint, HashMap<String, String> httpHeaders, boolean enableCompression)
        throws IOException {
        return doConnect(endpoint, httpHeaders, enableCompression);
    }

    public static void resetServerInvocationCount() {
        serverInvocationCount = 0;
    }

    private OutputStream doConnect(String endpoint, HashMap<String, String> httpHeaders, boolean enableCompression)
        throws IOException {
        this.httpMethod = new HttpPost(endpoint);
        for (String name : httpHeaders.keySet()) {
            this.httpMethod.addHeader(name, httpHeaders.get(name));
        }

        this.httpMethod.addHeader("User-Agent", VersionInfo.info());

        if (enableCompression && currentConfig.isCompression()) {
            this.httpMethod.addHeader("Content-Encoding", "gzip");
            this.httpMethod.addHeader("Accept-Encoding", "gzip");
        }

        entityByteOut = new ByteArrayOutputStream();
        OutputStream output = entityByteOut;

        if (currentConfig.getMaxRequestSize() > 0) {
            output = new LimitingOutputStream(currentConfig.getMaxRequestSize(), output);
        }

        if (enableCompression && currentConfig.isCompression()) {
            output = new GZIPOutputStream(output);
        }

        if (currentConfig.isTraceMessage()) {
            output = currentConfig.teeOutputStream(output);
        }

        if (currentConfig.hasMessageHandlers()) {
            URL url = new URL(endpoint);
            output = new MessageHandlerOutputStream(currentConfig, url, output);
        }

        return output;
    }

    public static void closeConnections() {
        if (currentHttpClient != null) {
            try {
                currentHttpClient.close();
            } catch (IOException ex) {
                // do nothing
            }
            currentHttpClient = null;
        }
    }
}