package io.odpf.depot.bigquery.handler;

import com.google.api.client.util.DateTime;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.bigquery.models.Records;
import io.odpf.depot.common.TupleString;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.DeserializerException;
import io.odpf.depot.exception.EmptyMessageException;
import io.odpf.depot.exception.UnknownFieldsException;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class MessageRecordConverter {
    private final OdpfMessageParser parser;
    private final BigQuerySinkConfig config;
    private final OdpfMessageSchema schema;

    public Records convert(List<OdpfMessage> messages) {
        ArrayList<Record> validRecords = new ArrayList<>();
        ArrayList<Record> invalidRecords = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            OdpfMessage message = messages.get(index);
            try {
                Record record = createRecord(message, index);
                validRecords.add(record);
            } catch (UnknownFieldsException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            } catch (EmptyMessageException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            } catch (DeserializerException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            } catch (UnsupportedOperationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            }
        }
        return new Records(validRecords, invalidRecords);
    }

    private Record createRecord(OdpfMessage message, int index) {
        try {
            SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? config.getSinkConnectorSchemaMessageClass() : config.getSinkConnectorSchemaKeyClass();
            ParsedOdpfMessage parsedOdpfMessage = parser.parse(message, mode, schemaClass);
            parsedOdpfMessage.validate(config);
            Map<String, Object> columns = parsedOdpfMessage.getMapping(schema);
            if (config.shouldAddMetadata()) {
                addMetadata(columns, message);
            }
            if (config.getSinkBigquerySchemaJsonOutputAddEventTimestampEnable()) {
                columns.put("event_timestamp", new Timestamp(System.currentTimeMillis()));
            }
            return new Record(message.getMetadata(), columns, index, null);
        } catch (IOException e) {
            log.error("failed to deserialize message: {}, {} ", e, message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }

    private void addMetadata(Map<String, Object> columns, OdpfMessage message) {
        List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
        Map<String, Object> metadata = message.getMetadata(metadataColumnsTypes);
        Map<String, Object> finalMetadata = metadataColumnsTypes.stream().collect(Collectors.toMap(TupleString::getFirst, t -> {
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
