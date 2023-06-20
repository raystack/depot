package com.gotocompany.depot.http;

import com.gotocompany.depot.common.client.HttpClientUtils;
import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.gotocompany.depot.http.client.HttpSinkClient;
import com.gotocompany.depot.http.request.Request;
import com.gotocompany.depot.http.request.RequestFactory;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.HttpSinkMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
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
            HttpSinkMetrics httpSinkMetrics = new HttpSinkMetrics(sinkConfig);
            httpSinkClient = new HttpSinkClient(closeableHttpClient, httpSinkMetrics, new Instrumentation(statsDReporter, HttpSinkClient.class));
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
            request = RequestFactory.create(sinkConfig, messageParser);
        } catch (ConfigurationException | InvalidTemplateException e) {
            throw new IllegalArgumentException("Exception occurred while creating Http sink", e);
        }
    }

    public Sink create() {
        return new HttpSink(
                httpSinkClient,
                request,
                sinkConfig.getSinkHttpRetryStatusCodeRanges(),
                sinkConfig.getSinkHttpRequestLogStatusCodeRanges(),
                new Instrumentation(statsDReporter, HttpSink.class)
        );
    }
}
