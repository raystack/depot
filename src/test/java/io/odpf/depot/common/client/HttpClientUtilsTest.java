package io.odpf.depot.common.client;

import io.odpf.depot.config.HttpClientConfig;
import io.odpf.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.gradle.internal.impldep.org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

public class HttpClientUtilsTest {

    private static ClientAndServer mockServer;
    @Mock
    private StatsDReporter statsDReporter;
    private HttpGet httpRequest;

    private HttpClientConfig clientConfig;

    @Before
    public void setup() {
        initMocks(this);
    }

    @BeforeClass
    public static void startServer() {
        mockServer = startClientAndServer(1080);
    }

    @AfterClass
    public static void stopServer() {
        mockServer.stop();
    }

    @org.junit.Before
    public void startMockServer() {
        mockServer.reset();
        mockServer.when(request().withPath("/oauth2/token"))
                .respond(response().withStatusCode(200).withBody("{\"access_token\":\"ACCESSTOKEN\",\"expires_in\":3599,\"scope\":\"order:read order:write\",\"token_type\":\"bearer\"}"));
        mockServer.when(request().withPath("/api"))
                .respond(response().withStatusCode(200).withBody("OK"));
    }

    @Test(expected = Test.None.class)
    public void shouldNotEmbedAccessTokenIfGoAuthDisabled() throws IOException {
        httpRequest = new HttpGet("http://127.0.0.1:1080/api");
        httpRequest.addHeader("foo", "bar");

        Map<String, String> configuration = new HashMap<>();
        configuration.put("HTTP_OAUTH2_ENABLE", "false");
        configuration.put("HTTP_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        clientConfig = ConfigFactory.create(HttpClientConfig.class, configuration);
        CloseableHttpClient closeableHttpClient = HttpClientUtils.newHttpClient(clientConfig, statsDReporter);

        closeableHttpClient.execute(httpRequest);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(0));
    }

    @Test(expected = Test.None.class)
    public void shouldEmbedAccessTokenIfGoAuthEnabled() throws IOException {
        httpRequest = new HttpGet("http://127.0.0.1:1080/api");
        httpRequest.addHeader("foo", "bar");

        Map<String, String> configuration = new HashMap<>();
        configuration.put("HTTP_OAUTH2_ENABLE", "true");
        configuration.put("HTTP_OAUTH2_ACCESS_TOKEN_URL", "http://127.0.0.1:1080/oauth2/token");
        clientConfig = ConfigFactory.create(HttpClientConfig.class, configuration);
        CloseableHttpClient closeableHttpClient = HttpClientUtils.newHttpClient(clientConfig, statsDReporter);

        closeableHttpClient.execute(httpRequest);

        mockServer.verify(request().withPath("/oauth2/token"), VerificationTimes.exactly(1));
    }
}
