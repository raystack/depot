package io.odpf.sink.connectors.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.sink.connectors.expcetion.EmptyMessageException;
import io.odpf.sink.connectors.message.InputSchemaMessageMode;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.message.OdpfMessageSchema;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import io.odpf.sink.connectors.stencil.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.metrics.StatsDReporter;
import io.odpf.sink.connectors.utils.StencilUtils;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.config.StencilConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ProtoOdpfMessageParser implements OdpfMessageParser {

    private final StencilClient stencilClient;
    private final ProtoFieldParser protoMappingParser = new ProtoFieldParser();

    public ProtoOdpfMessageParser(OdpfSinkConfig sinkConfig, StatsDReporter reporter, OdpfStencilUpdateListener protoUpdateListener) {
        StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, reporter.getClient(), protoUpdateListener);
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            stencilClient = StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            stencilClient = StencilClientFactory.getClient();
        }
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
                if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
                    log.info("empty message found {}", message.getMetadataString());
                    throw new EmptyMessageException();
                }
                dynamicMessage = stencilClient.parse(schemaClass, message.getLogMessage());
                break;
            case LOG_KEY:
                if (message.getLogKey() == null || message.getLogKey().length == 0) {
                    log.info("empty key found {}", message.getMetadataString());
                    throw new EmptyMessageException();
                }
                dynamicMessage = stencilClient.parse(schemaClass, message.getLogKey());
                break;
            default:
                throw new IOException("Error while parsing Message");
        }
        return new ProtoOdpfParsedMessage(dynamicMessage);
    }

    public Map<String, Descriptors.Descriptor> getDescriptorMap() {
        return stencilClient.getAll();
    }

    @Override
    public OdpfMessageSchema getSchema(String schemaClass) throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, getDescriptorMap(),
                getTypeNameToPackageNameMap(getDescriptorMap()));
        return new ProtoOdpfMessageSchema(protoField);
    }

    private Map<String, String> getTypeNameToPackageNameMap(Map<String, Descriptors.Descriptor> descriptors) {
        return descriptors.entrySet().stream()
                .collect(Collectors.toMap(
                        (mapEntry) -> String.format(".%s", mapEntry.getValue().getFullName()),
                        Map.Entry::getKey));
    }
}
