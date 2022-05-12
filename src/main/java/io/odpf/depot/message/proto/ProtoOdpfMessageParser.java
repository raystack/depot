package io.odpf.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.utils.StencilUtils;
import io.odpf.stencil.StencilClientFactory;
import io.odpf.stencil.client.StencilClient;
import io.odpf.stencil.config.StencilConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
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

    public ParsedOdpfMessage parse(OdpfMessage message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException {
        if (type == null) {
            throw new IOException("parser mode not defined");
        }
        byte[] payload;
        switch (type) {
            case LOG_MESSAGE:
                payload = (byte[]) message.getLogMessage();
                break;
            case LOG_KEY:
                payload = (byte[]) message.getLogKey();
                break;
            default:
                throw new ConfigurationException("Schema type not supported");
        }
        if (payload == null || payload.length == 0) {
            log.info("empty message found {}", message.getMetadataString());
            throw new EmptyMessageException();
        }
        DynamicMessage dynamicMessage = stencilClient.parse(schemaClass, payload);
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
                .filter(distinctByFullName(t -> t.getValue().getFullName()))
                .collect(Collectors.toMap(
                        (mapEntry) -> String.format(".%s", mapEntry.getValue().getFullName()),
                        Map.Entry::getKey));
    }

    private <T> Predicate<T> distinctByFullName(Function<? super T, Object> keyExtractor) {
        Set<Object> objects = new HashSet<>();
        return t -> objects.add(keyExtractor.apply(t));
    }
}
