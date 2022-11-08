package io.odpf.depot.metrics;

import io.odpf.depot.config.OdpfSinkConfig;

public class BigTableMetrics extends SinkMetrics {

    public static final String BIGTABLE_SINK_PREFIX = "bigtable_";
    public static final String BIGTABLE_INSTANCE_TAG = "instance=%s";
    public static final String BIGTABLE_TABLE_TAG = "table=%s";
    public static final String BIGTABLE_ERROR_TAG = "error=%s";

    public BigTableMetrics(OdpfSinkConfig config) {
        super(config);
    }


    public enum BigTableErrorType {
        QUOTA_FAILURE, // A quota check failed.
        PRECONDITION_FAILURE, // Some preconditions have failed.
        BAD_REQUEST, // Violations in a client request
        RPC_FAILURE,
    }

    public String getBigtableOperationLatencyMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "operation_latency_milliseconds";
    }

    public String getBigtableOperationTotalMetric() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "operation_total";
    }

    public String getBigtableTotalErrorsMetrics() {
        return getApplicationPrefix() + SINK_PREFIX + BIGTABLE_SINK_PREFIX + "errors_total";
    }
}
