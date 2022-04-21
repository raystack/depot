package io.odpf.sink.connectors.message;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.sink.connectors.bigquery.proto.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.metrics.StatsDReporter;
import io.odpf.sink.connectors.utils.StencilUtils;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.config.StencilConfig;

import java.io.IOException;
import java.util.Map;

public class ProtoOdpfMessageParser implements OdpfMessageParser {

    private final StencilClient stencilClient;

    public ProtoOdpfMessageParser(OdpfSinkConfig sinkConfig, StatsDReporter reporter, OdpfStencilUpdateListener protoUpdateListener) {
        StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, reporter.getClient(), protoUpdateListener);
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            stencilClient = StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            stencilClient = StencilClientFactory.getClient();
        }
    }

    public Map<String, Descriptors.Descriptor> getDescriptorMap() {
        return stencilClient.getAll();
    }

    public ProtoOdpfMessageParser(StencilClient stencilClient) {
        this.stencilClient = stencilClient;
    }

    public ParsedOdpfMessage parse(OdpfMessage message, InputSchemaMessageMode type, String schemaClass) throws IOException {
        if (type == null) {
            throw new IOException("parser mode not defined");
        }
        DynamicMessage dynamicMessage;
        switch (type) {
            case LOG_MESSAGE:
                dynamicMessage = stencilClient.parse(schemaClass, message.getLogMessage());
                break;
            case LOG_KEY:
                dynamicMessage = stencilClient.parse(schemaClass, message.getLogKey());
                break;
            default:
                throw new IOException("Error while parsing Message");
        }
        return new ProtoOdpfParsedMessage(dynamicMessage);
    }
}
