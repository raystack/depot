package com.gotocompany.depot.metrics;

import com.gotocompany.depot.config.SinkConfig;

public class HttpSinkMetrics extends SinkMetrics {

    public static final String HTTP_SINK_PREFIX = "http_";

    public HttpSinkMetrics(SinkConfig config) {
        super(config);
    }

    public String getHttpResponseCodeTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + HTTP_SINK_PREFIX + "response_code_total";
    }
}
