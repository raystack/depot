package org.raystack.depot.message;

import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.message.json.JsonRaystackMessageParser;
import org.raystack.depot.message.proto.ProtoRaystackMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.JsonParserMetrics;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.RaystackStencilUpdateListener;

public class RaystackMessageParserFactory {
    public static RaystackMessageParser getParser(RaystackSinkConfig config, StatsDReporter statsDReporter,
            RaystackStencilUpdateListener raystackStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonRaystackMessageParser(config,
                        new Instrumentation(statsDReporter, JsonRaystackMessageParser.class),
                        new JsonParserMetrics(config));
            case PROTOBUF:
                return new ProtoRaystackMessageParser(config, statsDReporter, raystackStencilUpdateListener);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    public static RaystackMessageParser getParser(RaystackSinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
