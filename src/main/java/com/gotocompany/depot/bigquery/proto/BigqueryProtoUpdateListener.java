package com.gotocompany.depot.bigquery.proto;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.protobuf.Descriptors.Descriptor;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverter;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.exception.BQSchemaMappingException;
import com.gotocompany.depot.bigquery.exception.BQTableUpdateFailure;
import com.gotocompany.depot.common.TupleString;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.message.proto.ProtoField;
import com.gotocompany.depot.message.proto.ProtoMessageParser;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class BigqueryProtoUpdateListener extends DepotStencilUpdateListener {
    private final BigQuerySinkConfig config;
    private final BigQueryClient bqClient;
    @Getter
    private final MessageRecordConverterCache converterCache;

    public BigqueryProtoUpdateListener(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache converterCache) {
        this.config = config;
        this.bqClient = bqClient;
        this.converterCache = converterCache;
    }

    @Override
    public void onSchemaUpdate(Map<String, Descriptor> newDescriptors) {
        log.info("stencil cache was refreshed, validating if bigquery schema changed");
        try {
            SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
            ProtoMessageParser messageParser = (ProtoMessageParser) getMessageParser();
            ProtoField protoField;
            if (newDescriptors == null) {
                protoField = messageParser.getProtoField(schemaClass);
            } else {
                protoField = messageParser.getProtoField(schemaClass, newDescriptors);
            }
            List<Field> bqSchemaFields = BigqueryFields.generateBigquerySchema(protoField);
            addMetadataFields(bqSchemaFields);
            bqClient.upsertTable(bqSchemaFields);
            converterCache.setMessageRecordConverter(new MessageRecordConverter(messageParser, config, null));
        } catch (BigQueryException | IOException e) {
            String errMsg = "Error while updating bigquery table on callback:" + e.getMessage();
            log.error(errMsg);
            throw new BQTableUpdateFailure(errMsg, e);
        }
    }

    @Override
    public void updateSchema() {
        onSchemaUpdate(null);
    }

    private void addMetadataFields(List<Field> bqSchemaFields) {
        List<Field> bqMetadataFields = new ArrayList<>();
        String namespaceName = config.getBqMetadataNamespace();
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            if (namespaceName.isEmpty()) {
                bqMetadataFields.addAll(BigqueryFields.getMetadataFields(metadataColumnsTypes));
            } else {
                bqMetadataFields.add(BigqueryFields.getNamespacedMetadataField(namespaceName, metadataColumnsTypes));
            }
        }

        List<String> duplicateFields = getDuplicateFields(bqSchemaFields, bqMetadataFields).stream().map(Field::getName).collect(Collectors.toList());
        if (duplicateFields.size() > 0) {
            throw new BQSchemaMappingException(String.format("Metadata field(s) is already present in the schema. "
                    + "fields: %s", duplicateFields));
        }
        bqSchemaFields.addAll(bqMetadataFields);
    }

    public void close() throws IOException {
    }

    private List<Field> getDuplicateFields(List<Field> fields1, List<Field> fields2) {
        return fields1.stream().filter(field -> containsField(fields2, field.getName())).collect(Collectors.toList());
    }

    private boolean containsField(List<Field> fields, String fieldName) {
        return fields.stream().anyMatch(field -> field.getName().equals(fieldName));
    }

}
