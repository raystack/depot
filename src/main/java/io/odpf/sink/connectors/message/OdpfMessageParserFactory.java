package io.odpf.sink.connectors.message;

import io.odpf.sink.connectors.bigquery.proto.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.message.json.JsonOdpfMessageParser;
import io.odpf.sink.connectors.message.proto.ProtoOdpfMessageParser;
import io.odpf.sink.connectors.metrics.StatsDReporter;

public class OdpfMessageParserFactory {
    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter, OdpfStencilUpdateListener odpfStencilUpdateListener) {
        switch (config.getInputSchemaDataTye()) {
            case JSON:
                return new JsonOdpfMessageParser(config);
            case PROTOBUF:
                ProtoOdpfMessageParser protoOdpfMessageParser = new ProtoOdpfMessageParser(config, statsDReporter, odpfStencilUpdateListener);
                if (odpfStencilUpdateListener != null) {
                    odpfStencilUpdateListener.setMessageParser(protoOdpfMessageParser);
                    odpfStencilUpdateListener.onSchemaUpdate(protoOdpfMessageParser.getDescriptorMap());
                }
                return protoOdpfMessageParser;
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }

    public static OdpfMessageParser getParser(OdpfSinkConfig config, StatsDReporter statsDReporter) {
        return getParser(config, statsDReporter, null);
    }
}
