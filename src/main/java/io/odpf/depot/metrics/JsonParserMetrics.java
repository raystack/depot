package io.odpf.depot.metrics;

import io.odpf.depot.config.OdpfSinkConfig;

public class JsonParserMetrics extends SinkMetrics {
    public JsonParserMetrics(OdpfSinkConfig config) {
        super(config);
    }

    public static final String JSON_PARSE_PREFIX = "json_parse_";

    public String getJsonParseTimeTakenMetric() {
        return getApplicationPrefix() + SINK_PREFIX + JSON_PARSE_PREFIX + "operation_milliseconds";
    }
}
