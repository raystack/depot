package org.raystack.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.exception.EmptyMessageException;
import org.raystack.depot.message.MessageUtils;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.RaystackStencilUpdateListener;
import org.raystack.depot.utils.StencilUtils;
import org.raystack.stencil.StencilClientFactory;
import org.raystack.stencil.client.StencilClient;
import org.raystack.stencil.config.StencilConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class ProtoRaystackMessageParser implements RaystackMessageParser {

    private final StencilClient stencilClient;
    private final ProtoFieldParser protoMappingParser = new ProtoFieldParser();

    public ProtoRaystackMessageParser(RaystackSinkConfig sinkConfig, StatsDReporter reporter,
            RaystackStencilUpdateListener protoUpdateListener) {
        StencilConfig stencilConfig = StencilUtils.getStencilConfig(sinkConfig, reporter.getClient(),
                protoUpdateListener);
        if (sinkConfig.isSchemaRegistryStencilEnable()) {
            stencilClient = StencilClientFactory.getClient(sinkConfig.getSchemaRegistryStencilUrls(), stencilConfig);
        } else {
            stencilClient = StencilClientFactory.getClient();
        }
    }

    public ProtoRaystackMessageParser(StencilClient stencilClient) {
        this.stencilClient = stencilClient;
    }

    public ParsedRaystackMessage parse(RaystackMessage message, SinkConnectorSchemaMessageMode type, String schemaClass)
            throws IOException {
        if (type == null) {
            throw new IOException("parser mode not defined");
        }
        MessageUtils.validate(message, byte[].class);
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
        return new ProtoRaystackParsedMessage(dynamicMessage);
    }

    public Map<String, Descriptors.Descriptor> getDescriptorMap() {
        return stencilClient.getAll();
    }

    @Override
    public RaystackMessageSchema getSchema(String schemaClass) throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, getDescriptorMap(),
                getTypeNameToPackageNameMap(getDescriptorMap()));
        return new ProtoRaystackMessageSchema(protoField);
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

    public RaystackMessageSchema getSchema(String schemaClass, Map<String, Descriptors.Descriptor> newDescriptors)
            throws IOException {
        ProtoField protoField = new ProtoField();
        protoField = protoMappingParser.parseFields(protoField, schemaClass, newDescriptors,
                getTypeNameToPackageNameMap(newDescriptors));
        return new ProtoRaystackMessageSchema(protoField);
    }
}
