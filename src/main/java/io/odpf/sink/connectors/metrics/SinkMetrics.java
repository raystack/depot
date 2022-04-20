package io.odpf.sink.connectors.metrics;

import io.odpf.sink.connectors.config.OdpfSinkConfig;

public class SinkMetrics {
    public static final String SINK_PREFIX = "sink_";
    // ERROR TAGS
    public static final String ERROR_TYPE_TAG = "error_type=%s";
    public static final String ERROR_PREFIX = "error_";
    public static final String ERROR_MESSAGE_CLASS_TAG = "class";
    public static final String NON_FATAL_ERROR = "nonfatal";
    public static final String FATAL_ERROR = "fatal";

    protected String applicationPrefix;

    public SinkMetrics(OdpfSinkConfig config) {
        this.applicationPrefix = config.getMetricsApplicationPrefix();
    }

    public String getErrorEventMetric() {
        return applicationPrefix + ERROR_PREFIX + "event";
    }
}
