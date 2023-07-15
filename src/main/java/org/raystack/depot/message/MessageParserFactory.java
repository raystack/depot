package org.raystack.depot.message;

import org.raystack.depot.config.SinkConfig;
import org.raystack.depot.message.json.JsonMessageParser;
import org.raystack.depot.message.proto.ProtoMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.JsonParserMetrics;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.StencilUpdateListener;

public class MessageParserFactory {
    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter,
            StencilUpdateListener raystackStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonMessageParser(config,
                        new Instrumentation(statsDReporter, JsonMessageParser.class),
                        new JsonParserMetrics(config));
            case PROTOBUF:
                return new ProtoMessageParser(config, statsDReporter, raystackStencilUpdateListener);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
