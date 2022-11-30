package io.odpf.depot.http;

import com.timgroup.statsd.NoOpStatsDClient;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.common.client.HttpClientUtils;
import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.http.client.HttpSinkClient;
import io.odpf.depot.http.request.Request;
import io.odpf.depot.http.request.RequestFactory;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import org.apache.http.impl.client.CloseableHttpClient;

public class HttpSinkFactory {

    private final HttpSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;

    private HttpSinkClient httpSinkClient;
    private Request request;

    public HttpSinkFactory(HttpSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public HttpSinkFactory(HttpSinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = new StatsDReporter(new NoOpStatsDClient());
    }

    public void init() {
        try {
            CloseableHttpClient closeableHttpClient = HttpClientUtils.newHttpClient(sinkConfig, statsDReporter);
            httpSinkClient = new HttpSinkClient(closeableHttpClient, new Instrumentation(statsDReporter, HttpSinkClient.class));
            request = RequestFactory.create(sinkConfig);
        } catch (InvalidTemplateException e) {
            throw new IllegalArgumentException("Exception occurred while creating Http sink", e);
        }
    }

    public OdpfSink create() {
        return new HttpSink(
                httpSinkClient,
                request,
                new Instrumentation(statsDReporter, HttpSink.class)
        );
    }
}
