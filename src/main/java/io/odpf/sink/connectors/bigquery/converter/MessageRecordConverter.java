package io.odpf.sink.connectors.bigquery.converter;

import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.bigquery.models.Records;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.error.ErrorType;
import io.odpf.sink.connectors.expcetion.DeserializerException;
import io.odpf.sink.connectors.expcetion.EmptyMessageException;
import io.odpf.sink.connectors.expcetion.UnknownFieldsException;
import io.odpf.sink.connectors.message.InputSchemaMessageMode;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.message.OdpfMessageSchema;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
            }
        }
        return new Records(validRecords, invalidRecords);
    }

    private Record createRecord(OdpfMessage message, int index) {
        try {
            InputSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == InputSchemaMessageMode.LOG_MESSAGE
                    ? config.getSinkConnectorSchemaMessageClass() : config.getSinkConnectorSchemaKeyClass();
            ParsedOdpfMessage parsedOdpfMessage = parser.parse(message, mode, schemaClass);
            parsedOdpfMessage.validate(config);
            Map<String, Object> columns = parsedOdpfMessage.getMapping(schema);
            if (config.shouldAddMetadata()) {
                addMetadata(columns, message);
            }
            return new Record(message.getMetadata(), columns, index, null);
        } catch (IOException e) {
            log.error("failed to deserialize message: {}, {} ", e, message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }

    private void addMetadata(Map<String, Object> columns, OdpfMessage message) {
        if (config.getBqMetadataNamespace().isEmpty()) {
            columns.putAll(message.getMetadata());
        } else {
            columns.put(config.getBqMetadataNamespace(), message.getMetadata());
        }
    }
}
