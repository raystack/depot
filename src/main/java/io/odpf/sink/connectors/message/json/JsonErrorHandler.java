package io.odpf.sink.connectors.message.json;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import io.odpf.sink.connectors.bigquery.error.ErrorHandler;
import io.odpf.sink.connectors.bigquery.handler.BigQueryClient;
import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.error.ErrorType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

/*
the job of the class is to handle unknown field errors and then update the bq table schema,
 this happens incase of where schema is inferred from incoming data
 */
public class JsonErrorHandler implements ErrorHandler {

    private final BigQueryClient bigQueryClient;
    private final String tablePartitionKey;
    private final boolean castAllColumnsToStringDataType;

    public JsonErrorHandler(BigQueryClient bigQueryClient, String tablePartitionKey, boolean castAllColumnsToStringDataType) {

        this.bigQueryClient = bigQueryClient;
        this.tablePartitionKey = tablePartitionKey;
        this.castAllColumnsToStringDataType = castAllColumnsToStringDataType;
    }

    @Override
    public void handle(Map<Long, ErrorInfo> errorInfoMap, List<Record> records) {
        Schema schema = bigQueryClient.getSchema();
        FieldList existingFieldList = schema.getFields();
        HashMap<Integer, ErrorInfo> integerErrorInfoMap = new HashMap<>();
        errorInfoMap.forEach((k, v) -> integerErrorInfoMap.put(k.intValue(), v));

        List<Entry<Integer, ErrorInfo>> unknownFieldErrors = getUnknownFieldErrors(integerErrorInfoMap);
        if (unknownFieldErrors.size() > 0) {
            Set<Field> missingFields = unknownFieldErrors
                    .stream()
                    .map(x -> getColumnNamesForRecordsWhichHadUknownFieldErrors(records, x))
                    .flatMap(keys -> keys.stream())
                    .filter(key -> filterExistingFields(existingFieldList, key))
                    .map(key -> getField(key))
                    .collect(Collectors.toSet());

            ArrayList<Field> bqSchemaFields = new ArrayList<>(missingFields);
            existingFieldList.iterator().forEachRemaining(bqSchemaFields::add);
            bigQueryClient.upsertTable(bqSchemaFields);
        }
    }

    private Set<String> getColumnNamesForRecordsWhichHadUknownFieldErrors(List<Record> records, Entry<Integer, ErrorInfo> x) {
        Integer recordKey = x.getKey();
        return records.get(recordKey).getColumns().keySet();
    }

    private Field getField(String key) {
        if(tablePartitionKey != null && tablePartitionKey.equals(key)) {
            return Field.of(key, LegacySQLTypeName.TIMESTAMP);
        }
        if(!castAllColumnsToStringDataType) {
            throw new UnsupportedOperationException("only string data type is supported for fields other than partition key");
        }
        return Field.of(key, LegacySQLTypeName.STRING);
    }

    private boolean filterExistingFields(FieldList existingFieldList, String key) {
        try {
            existingFieldList.get(key);
            return false;
        }
        catch (IllegalArgumentException ex) {
            return true;
        }
    }

    private List<Entry<Integer, ErrorInfo>> getUnknownFieldErrors(Map<Integer, ErrorInfo> errorInfoMap) {
        return errorInfoMap.entrySet()
                .stream()
                .filter(errorInfoEntry -> errorInfoEntry.getValue().getErrorType() == ErrorType.UNKNOWN_FIELDS_ERROR)
                .collect(Collectors.toList());
    }
}
