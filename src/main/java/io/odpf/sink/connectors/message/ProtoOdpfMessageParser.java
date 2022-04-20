package io.odpf.sink.connectors.message;

import com.google.protobuf.DynamicMessage;
import io.odpf.sink.connectors.bigquery.proto.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.metrics.StatsDReporter;
import io.odpf.sink.connectors.utils.StencilUtils;
import io.odpf.stencil.Parser;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.config.StencilConfig;

import java.io.IOException;

public class ProtoOdpfMessageParser implements OdpfMessageParser {

    private final Parser parser;

    public ProtoOdpfMessageParser(OdpfSinkConfig sinkConfig, StatsDReporter reporter, OdpfStencilUpdateListener protoUpdateListener) {
        StencilClient stencilClient;
        StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, reporter.getClient(), protoUpdateListener);
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            stencilClient = StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            stencilClient = StencilClientFactory.getClient();
        }
        this.parser = stencilClient.getParser(sinkConfig.getInputSchemaProtoClass());
        if (protoUpdateListener != null) {
            protoUpdateListener.setMessageParser(this);
            protoUpdateListener.onSchemaUpdate(stencilClient.getAll());
        }
    }

    public ProtoOdpfMessageParser(Parser parser) {
        this.parser = parser;
    }

    public ParsedOdpfMessage parse(OdpfMessage message, InputSchemaMessageMode type) throws IOException {
        DynamicMessage dynamicMessage = null;
        switch (type) {
            case LOG_MESSAGE:
                dynamicMessage = parser.parse(message.getLogMessage());
                break;
            case LOG_KEY:
                dynamicMessage = parser.parse(message.getLogKey());
                break;
            default:
                throw new IOException("Error while parsing Message");
        }
        return new ProtoOdpfParsedMessage(dynamicMessage);
    }
}
