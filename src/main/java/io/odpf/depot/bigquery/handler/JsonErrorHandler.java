package io.odpf.depot.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.common.TupleString;
import io.odpf.depot.config.BigQuerySinkConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/*
the job of the class is to handle unknown field errors and then update the bq table schema,
 this happens incase of where schema is inferred from incoming data
 */
public class JsonErrorHandler implements ErrorHandler {

    private final BigQueryClient bigQueryClient;
    private final String tablePartitionKey;
    private final Optional<LegacySQLTypeName> partitionKeyDataType;
    private final boolean castAllColumnsToStringDataType;
    private final Map<String, String> metadataColumnsTypesMap;
    private final String bqMetadataNamespace;

    public JsonErrorHandler(BigQueryClient bigQueryClient, BigQuerySinkConfig bigQuerySinkConfig) {

        this.bigQueryClient = bigQueryClient;
        tablePartitionKey = bigQuerySinkConfig.isTablePartitioningEnabled() ? bigQuerySinkConfig.getTablePartitionKey() : "";
        if (bigQuerySinkConfig.isTablePartitioningEnabled()) {
            List<TupleString> defaultColumns = bigQuerySinkConfig.getSinkBigquerySchemaJsonOutputDefaultColumns();
            partitionKeyDataType = defaultColumns
                    .stream()
                    .filter(column -> tablePartitionKey.equals(column.getFirst()))
                    .findFirst()
                    .map(tupleString -> LegacySQLTypeName.valueOfStrict(tupleString.getSecond().toUpperCase()));
        } else {
            partitionKeyDataType = Optional.empty();
        }
        castAllColumnsToStringDataType = bigQuerySinkConfig.getSinkConnectorSchemaJsonOutputDefaultDatatypeStringEnable();
        bqMetadataNamespace = bigQuerySinkConfig.getBqMetadataNamespace();
        metadataColumnsTypesMap = bigQuerySinkConfig
                .getMetadataColumnsTypes()
                .stream()
                .collect(Collectors.toMap(TupleString::getFirst, TupleString::getSecond));
    }

    public void handle(Map<Long, List<BigQueryError>> insertErrors, List<Record> records) {

        Schema schema = bigQueryClient.getSchema();
        FieldList existingFieldList = schema.getFields();
        List<Entry<Long, List<BigQueryError>>> unknownFieldBqErrors = getUnknownFieldBqErrors(insertErrors);
        if (!unknownFieldBqErrors.isEmpty()) {
            Set<Field> missingFields = unknownFieldBqErrors
                    .parallelStream()
                    .map(x -> getColumnNamesForRecordsWhichHadUknownBqFieldErrors(records, x))
                    .flatMap(keys -> keys.stream())
                    .filter(key -> filterExistingFields(existingFieldList, key))
                    .map(key -> getField(key))
                    .collect(Collectors.toSet());
            ArrayList<Field> bqSchemaFields = new ArrayList<>(missingFields);
            existingFieldList.iterator().forEachRemaining(bqSchemaFields::add);
            bigQueryClient.upsertTable(bqSchemaFields);
        }
    }

    private Set<String> getColumnNamesForRecordsWhichHadUknownBqFieldErrors(List<Record> records, Entry<Long, List<BigQueryError>> x) {
        Integer recordKey = x.getKey().intValue();
        return records.get(recordKey).getColumns().keySet();
    }


    private List<Entry<Long, List<BigQueryError>>> getUnknownFieldBqErrors(Map<Long, List<BigQueryError>> insertErrors) {
        List<Entry<Long, List<BigQueryError>>> unkownFieldFieldBqError = insertErrors.entrySet().parallelStream()
                .filter((x) -> {
                    List<BigQueryError> value = x.getValue();
                    List<BigQueryError> bqErrorsWithNoSuchFields = getBqErrorsWithNoSuchFields(value);
                    if (!bqErrorsWithNoSuchFields.isEmpty()) {
                        return true;
                    }
                    return false;

                }).collect(Collectors.toList());
        return unkownFieldFieldBqError;
    }

    private List<BigQueryError> getBqErrorsWithNoSuchFields(List<BigQueryError> value) {
        return value.stream().filter(
                bigQueryError -> {
                    if (bigQueryError.getReason().equals("invalid")
                            && bigQueryError.getMessage().contains("no such field")) {
                        return true;
                    }
                    return false;
                }
        ).collect(Collectors.toList());
    }


    private Field getField(String key) {
        if ((!tablePartitionKey.isEmpty()) && tablePartitionKey.equals(key)) {
            return Field.of(key, partitionKeyDataType.get());
        }
        if (!bqMetadataNamespace.isEmpty()) {
            throw new UnsupportedOperationException("metadata namespace is not supported, because nested json structure is not supported");
        }
        if (metadataColumnsTypesMap.containsKey(key)) {
            return Field.of(key, LegacySQLTypeName.valueOfStrict(metadataColumnsTypesMap.get(key).toUpperCase()));
        }
        if (!castAllColumnsToStringDataType) {
            throw new UnsupportedOperationException("only string data type is supported for fields other than partition key");
        }
        return Field.of(key, LegacySQLTypeName.STRING);
    }

    private boolean filterExistingFields(FieldList existingFieldList, String key) {
        try {
            existingFieldList.get(key);
            return false;
        } catch (IllegalArgumentException ex) {
            return true;
        }
    }

}
