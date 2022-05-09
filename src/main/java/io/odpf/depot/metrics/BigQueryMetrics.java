package io.odpf.depot.metrics;

import io.odpf.depot.config.OdpfSinkConfig;

public class BigQueryMetrics extends SinkMetrics {

    public BigQueryMetrics(OdpfSinkConfig config) {
        super(config);
    }

    public enum BigQueryAPIType {
        TABLE_UPDATE,
        TABLE_CREATE,
        DATASET_UPDATE,
        DATASET_CREATE,
        TABLE_INSERT_ALL,
    }

    public enum BigQueryErrorType {
        UNKNOWN_ERROR,
        INVALID_SCHEMA_ERROR,
        OOB_ERROR,
        STOPPED_ERROR,
    }

    public static final String BIGQUERY_SINK_PREFIX = "bigquery_";
    public static final String BIGQUERY_TABLE_TAG = "table=%s";
    public static final String BIGQUERY_DATASET_TAG = "dataset=%s";
    public static final String BIGQUERY_API_TAG = "api=%s";
    public static final String BIGQUERY_ERROR_TAG = "error=%s";

    public String getBigqueryOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_total";
    }

    public String getBigqueryOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "operation_latency_milliseconds";
    }

    public String getBigqueryTotalErrorsMetrics() {
        return getApplicationPrefix() + SINK_PREFIX + BIGQUERY_SINK_PREFIX + "errors_total";
    }
}
