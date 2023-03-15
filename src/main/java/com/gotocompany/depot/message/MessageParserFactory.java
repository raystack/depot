package com.gotocompany.depot.message;

import com.gotocompany.depot.message.json.JsonMessageParser;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.JsonParserMetrics;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import com.gotocompany.depot.config.SinkConfig;

public class MessageParserFactory {
    public static MessageParser getParser(SinkConfig config, StatsDReporter statsDReporter, DepotStencilUpdateListener depotStencilUpdateListener) {
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
