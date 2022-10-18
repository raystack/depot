package io.odpf.depot.common.client;

import io.odpf.depot.common.client.auth.OAuth2Credential;
import io.odpf.depot.config.HttpClientConfig;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HttpClientUtils {

    public static CloseableHttpClient newHttpClient(HttpClientConfig config, StatsDReporter statsDReporter) {
        Integer maxHttpConnections = config.getHttpMaxConnections();
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(config.getHttpRequestTimeoutMs())
                .setConnectionRequestTimeout(config.getHttpRequestTimeoutMs())
                .setConnectTimeout(config.getHttpRequestTimeoutMs()).build();
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        connectionManager.setMaxTotal(maxHttpConnections);
        connectionManager.setDefaultMaxPerRoute(maxHttpConnections);
        HttpClientBuilder builder = HttpClients.custom().setConnectionManager(connectionManager).setDefaultRequestConfig(requestConfig);
        if (config.isHttpOAuth2Enable()) {
            OAuth2Credential oauth2 = new OAuth2Credential(
                    new Instrumentation(statsDReporter, OAuth2Credential.class),
                    config.getHttpOAuth2ClientName(),
                    config.getHttpOAuth2ClientSecret(),
                    config.getHttpOAuth2Scope(),
                    config.getHttpOAuth2AccessTokenUrl());
            builder = oauth2.initialize(builder);
        }

        return builder.build();
    }
}
