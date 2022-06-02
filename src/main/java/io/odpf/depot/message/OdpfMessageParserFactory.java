package io.odpf.depot.message;

import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.json.JsonOdpfMessageParser;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.JsonParserMetrics;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;

public class OdpfMessageParserFactory {
    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter, OdpfStencilUpdateListener odpfStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonOdpfMessageParser(config,
                        new Instrumentation(statsDReporter, JsonOdpfMessageParser.class),
                        new JsonParserMetrics(config));
            case PROTOBUF:
                return new ProtoOdpfMessageParser(config, statsDReporter, odpfStencilUpdateListener);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
