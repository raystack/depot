package org.raystack.depot.bigquery.converter;

import org.raystack.depot.bigquery.models.Record;
import org.raystack.depot.bigquery.models.Records;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.error.ErrorType;
import org.raystack.depot.exception.DeserializerException;
import org.raystack.depot.exception.EmptyMessageException;
import org.raystack.depot.exception.UnknownFieldsException;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.message.RaystackMessageSchema;
import org.raystack.depot.message.ParsedRaystackMessage;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
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
    private final RaystackMessageParser parser;
    private final BigQuerySinkConfig config;
    private final RaystackMessageSchema schema;

    public Records convert(List<RaystackMessage> messages) {
        ArrayList<Record> validRecords = new ArrayList<>();
        ArrayList<Record> invalidRecords = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            RaystackMessage message = messages.get(index);
            try {
                Record record = createRecord(message, index);
                validRecords.add(record);
            } catch (UnknownFieldsException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            } catch (EmptyMessageException | UnsupportedOperationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            } catch (DeserializerException | IllegalArgumentException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                invalidRecords.add(new Record(message.getMetadata(), Collections.emptyMap(), index, errorInfo));
            }
        }
        return new Records(validRecords, invalidRecords);
    }

    private Record createRecord(RaystackMessage message, int index) {
        try {
            SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? config.getSinkConnectorSchemaProtoMessageClass()
                    : config.getSinkConnectorSchemaProtoKeyClass();
            ParsedRaystackMessage parsedRaystackMessage = parser.parse(message, mode, schemaClass);
            parsedRaystackMessage.validate(config);
            Map<String, Object> columns = parsedRaystackMessage.getMapping(schema);
            MessageRecordConverterUtils.addMetadata(columns, message, config);
            MessageRecordConverterUtils.addTimeStampColumnForJson(columns, config);
            return new Record(message.getMetadata(), columns, index, null);
        } catch (IOException e) {
            log.error("failed to deserialize message: {}, {} ", e, message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }
}