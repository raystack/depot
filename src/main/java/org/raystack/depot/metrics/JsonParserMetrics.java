package org.raystack.depot.metrics;

import org.raystack.depot.config.SinkConfig;

public class JsonParserMetrics extends SinkMetrics {
    public JsonParserMetrics(SinkConfig config) {
        super(config);
    }

    public static final String JSON_PARSE_PREFIX = "json_parse_";

    public String getJsonParseTimeTakenMetric() {
        return getApplicationPrefix() + SINK_PREFIX + JSON_PARSE_PREFIX + "operation_milliseconds";
    }
}
