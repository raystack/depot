package com.gotocompany.depot.bigquery.converter;

import com.google.api.client.util.DateTime;
import com.google.api.client.util.Preconditions;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.error.ErrorType;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.exception.EmptyMessageException;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.SchemaFieldType;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@AllArgsConstructor
@Slf4j
public class MessageRecordConverter {
    private final MessageParser parser;
    private final BigQuerySinkConfig config;


    public Records convert(List<Message> messages) {
        ArrayList<Record> validRecords = new ArrayList<>();
        ArrayList<Record> invalidRecords = new ArrayList<>();
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
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

    private Record createRecord(Message message, int index) {
        try {
            SinkConnectorSchemaMessageMode mode = config.getSinkConnectorSchemaMessageMode();
            String schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                    ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
            ParsedMessage parsedMessage = parser.parse(message, mode, schemaClass);
            parsedMessage.validate(config);
            Map<String, Object> columns = getMapping(parsedMessage);
            MessageRecordConverterUtils.addMetadata(columns, message, config);
            MessageRecordConverterUtils.addTimeStampColumnForJson(columns, config);
            return new Record(message.getMetadata(), columns, index, null);
        } catch (IOException e) {
            log.error("failed to deserialize message: {}, {} ", e, message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
        }
    }

    private void floatCheck(Object fieldValue) {
        if (fieldValue instanceof Float) {
            float floatValue = ((Number) fieldValue).floatValue();
            Preconditions.checkArgument(!Float.isInfinite(floatValue) && !Float.isNaN(floatValue));
        } else if (fieldValue instanceof Double) {
            double doubleValue = ((Number) fieldValue).doubleValue();
            Preconditions.checkArgument(!Double.isInfinite(doubleValue) && !Double.isNaN(doubleValue));
        }
    }

    private Object getFieldValue(SchemaField field, Object value) {
        if (field.getType().equals(SchemaFieldType.FLOAT) || field.getType().equals(SchemaFieldType.DOUBLE)) {
            floatCheck(value);
        }
        if (field.getType().equals(SchemaFieldType.BYTES)) {
            return BaseEncoding.base64().encode(((ByteString) value).toByteArray());
        }
        if (field.getType().equals(SchemaFieldType.MESSAGE)) {
            ParsedMessage msg = (ParsedMessage) value;
            if (msg.getSchema().logicalType().equals(LogicalType.TIMESTAMP)) {
                return new DateTime(msg.getLogicalValue().getTimestamp().toEpochMilli());
            }
            if (msg.getSchema().logicalType().equals(LogicalType.STRUCT)) {
                JSONObject json = new JSONObject(msg.getLogicalValue().getStruct());
                return json.toString();
            }
            return getMapping(msg);
        }
        return value;
    }

    private Object getListValue(SchemaField schemaField, List<?> value) {
        return value
                .stream()
                .map(eachValue -> getFieldValue(schemaField, eachValue))
                .collect(Collectors.toList());
    }

    private Object getValue(Map.Entry<SchemaField, Object> kv) {
        SchemaField schemaField = kv.getKey();
        Object value = kv.getValue();
        if (schemaField.isRepeated()) {
            return getListValue(schemaField, (List<?>) value);
        }
        return getFieldValue(schemaField, value);
    }

    private Map<String, Object> getMapping(ParsedMessage msg) {
        return msg
                .getFields()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(kv -> kv.getKey().getName(), this::getValue));
    }
}
