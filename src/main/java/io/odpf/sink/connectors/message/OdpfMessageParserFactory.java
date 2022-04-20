package io.odpf.sink.connectors.message;

import io.odpf.sink.connectors.bigquery.proto.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.metrics.StatsDReporter;

public class OdpfMessageParserFactory {
    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter, OdpfStencilUpdateListener odpfStencilUpdateListener) {
        switch (config.getInputSchemaDataTye()) {
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

