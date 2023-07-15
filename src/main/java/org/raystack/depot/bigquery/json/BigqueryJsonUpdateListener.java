package org.raystack.depot.bigquery.json;

import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.converter.MessageRecordConverter;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.bigquery.exception.BQTableUpdateFailure;
import org.raystack.depot.bigquery.proto.BigqueryFields;
import org.raystack.depot.common.TupleString;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.message.MessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.stencil.DepotStencilUpdateListener;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class BigqueryJsonUpdateListener extends DepotStencilUpdateListener {
    private final MessageRecordConverterCache converterCache;
    private final BigQuerySinkConfig config;
    private final BigQueryClient bigQueryClient;
    private final Instrumentation instrumentation;

    public BigqueryJsonUpdateListener(BigQuerySinkConfig config, MessageRecordConverterCache converterCache,
            BigQueryClient bigQueryClient, Instrumentation instrumentation) {
        this.converterCache = converterCache;
        this.config = config;
        this.bigQueryClient = bigQueryClient;
        this.instrumentation = instrumentation;
        if (!config.getSinkBigqueryDynamicSchemaEnable()) {
            throw new UnsupportedOperationException(
                    "currently only schema inferred from incoming data is supported, stencil schema support for json will be added in future");
        }
    }

    @Override
    public void updateSchema() {
        MessageParser parser = getMessageParser();
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(parser, config, null);
        converterCache.setMessageRecordConverter(messageRecordConverter);
        List<TupleString> defaultColumns = config.getSinkBigqueryDefaultColumns();
        HashSet<Field> fieldsToBeUpdated = defaultColumns
                .stream()
                .map(this::getField)
                .collect(Collectors.toCollection(HashSet::new));
        if (config.shouldAddMetadata() && !config.getBqMetadataNamespace().isEmpty()) {
            throw new UnsupportedOperationException(
                    "metadata namespace is not supported, because nested json structure is not supported");
        }
        addMetadataFields(fieldsToBeUpdated, defaultColumns);
        try {
            Schema existingTableSchema = bigQueryClient.getSchema();
            FieldList existingTableFields = existingTableSchema.getFields();
            existingTableFields.iterator().forEachRemaining(fieldsToBeUpdated::add);
            bigQueryClient.upsertTable(new ArrayList<>(fieldsToBeUpdated));
        } catch (BigQueryException e) {
            String errMsg = "Error while updating bigquery table in json update listener:" + e.getMessage();
            instrumentation.logError(errMsg);
            throw new BQTableUpdateFailure(errMsg, e);
        }
    }

    /*
     * throws error incase there are duplicate fields between metadata and default
     * columns config
     */
    private void addMetadataFields(HashSet<Field> fieldsToBeUpdated, List<TupleString> defaultColumns) {
        if (config.shouldAddMetadata()) {
            Set<String> defaultColumnNames = defaultColumns
                    .stream()
                    .map(TupleString::getFirst)
                    .collect(Collectors.toSet());
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            List<Field> metadataFields = BigqueryFields.getMetadataFieldsStrict(metadataColumnsTypes);
            Optional<Field> duplicateField = metadataFields
                    .stream()
                    .filter(m -> defaultColumnNames.contains(m.getName())).findFirst();
            if (duplicateField.isPresent()) {
                String duplicateFieldName = duplicateField.get().getName();
                instrumentation.logError("duplicate key found in default columns and metadata config {}",
                        duplicateFieldName);
                throw new IllegalArgumentException("duplicate field called "
                        + duplicateFieldName
                        + " is present in both default columns config and metadata config");
            }
            fieldsToBeUpdated.addAll(metadataFields);
        }
    }

    private Field getField(TupleString tupleString) {
        String fieldName = tupleString.getFirst();
        LegacySQLTypeName fieldDataType = LegacySQLTypeName.valueOfStrict(tupleString.getSecond().toUpperCase());
        return checkAndCreateField(fieldName, fieldDataType);
    }

    /**
     * Range BigQuery partitioning is not supported, supported partition fields have
     * to be of DATE or TIMESTAMP type..
     */
    private Field checkAndCreateField(String fieldName, LegacySQLTypeName fieldDataType) {
        Boolean isPartitioningEnabled = config.isTablePartitioningEnabled();
        if (!isPartitioningEnabled) {
            return Field.newBuilder(fieldName, fieldDataType).setMode(Field.Mode.NULLABLE).build();
        }
        String partitionKey = config.getTablePartitionKey();
        boolean isValidPartitionDataType = (fieldDataType == LegacySQLTypeName.TIMESTAMP
                || fieldDataType == LegacySQLTypeName.DATE);
        if (partitionKey.equals(fieldName) && !isValidPartitionDataType) {
            throw new UnsupportedOperationException(
                    "supported partition fields have to be of DATE or TIMESTAMP type..");
        }
        return Field.newBuilder(fieldName, fieldDataType).setMode(Field.Mode.NULLABLE).build();
    }
}
