package org.raystack.depot.bigquery.converter;

import com.google.api.client.util.DateTime;
import org.raystack.depot.common.TupleString;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.config.enums.SinkConnectorSchemaDataType;
import org.raystack.depot.message.OdpfMessage;
import org.raystack.depot.utils.DateUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageRecordConverterUtils {

    public static final String JSON_TIME_STAMP_COLUMN = "event_timestamp";

    public static void addMetadata(Map<String, Object> columns, OdpfMessage message, BigQuerySinkConfig config) {
        if (config.shouldAddMetadata()) {
            List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
            Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
            Map<String, Object> finalMetadata = metadataColumnsTypes.stream()
                    .collect(Collectors.toMap(TupleString::getFirst, t -> {
                        String key = t.getFirst();
                        String dataType = t.getSecond();
                        Object value = metadata.get(key);
                        if (value instanceof Long && dataType.equals("timestamp")) {
                            value = new DateTime((long) value);
                        }
                        return value;
                    }));
            if (config.getBqMetadataNamespace().isEmpty()) {
                columns.putAll(finalMetadata);
            } else {
                columns.put(config.getBqMetadataNamespace(), finalMetadata);
            }

        }
    }

    public static void addTimeStampColumnForJson(Map<String, Object> columns, BigQuerySinkConfig config) {
        if (config.getSinkConnectorSchemaDataType() == SinkConnectorSchemaDataType.JSON
                && config.getSinkBigqueryAddEventTimestampEnable()) {
            columns.put(JSON_TIME_STAMP_COLUMN, DateUtils.formatCurrentTimeAsUTC());
        }
    }
}
