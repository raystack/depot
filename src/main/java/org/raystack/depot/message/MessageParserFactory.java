package org.raystack.depot.message;

import org.raystack.depot.message.json.JsonMessageParser;
import org.raystack.depot.message.proto.ProtoMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.JsonParserMetrics;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.DepotStencilUpdateListener;
import org.raystack.depot.config.SinkConfig;

public class MessageParserFactory {
    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter,
            DepotStencilUpdateListener depotStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonMessageParser(config,
                        new Instrumentation(statsDReporter, JsonMessageParser.class),
                        new JsonParserMetrics(config));
            case PROTOBUF:
                return new ProtoMessageParser(config, statsDReporter, depotStencilUpdateListener);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
