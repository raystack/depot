package org.raystack.depot.message;

import org.raystack.depot.config.OdpfSinkConfig;
import org.raystack.depot.message.json.JsonOdpfMessageParser;
import org.raystack.depot.message.proto.ProtoOdpfMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.JsonParserMetrics;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.OdpfStencilUpdateListener;

public class OdpfMessageParserFactory {
    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter,
            OdpfStencilUpdateListener raystackStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonOdpfMessageParser(config,
                        new Instrumentation(statsDReporter, JsonOdpfMessageParser.class),
                        new JsonParserMetrics(config));
            case PROTOBUF:
                return new ProtoOdpfMessageParser(config, statsDReporter, raystackStencilUpdateListener);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
