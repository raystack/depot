package io.odpf.depot.message;

import io.odpf.depot.message.json.JsonOdpfMessageParser;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.proto.ProtoOdpfMessageParser;
import io.odpf.depot.metrics.StatsDReporter;

public class OdpfMessageParserFactory {
    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter, OdpfStencilUpdateListener odpfStencilUpdateListener) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new JsonOdpfMessageParser(config);
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
