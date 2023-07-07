package com.gotocompany.depot.bigquery.storage.proto;

import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
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
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class BigQueryProtoStorageClient implements BigQueryStorageClient {

    private final BigQueryProtoWriter writer;
    private final BigQuerySinkConfig config;
    private final MessageParser parser;
    private final String schemaClass;
    private final SinkConnectorSchemaMessageMode mode;

    public BigQueryProtoStorageClient(BigQueryWriter writer, BigQuerySinkConfig config, MessageParser parser) {
        this.writer = (BigQueryProtoWriter) writer;
        this.config = config;
        this.parser = parser;
        this.mode = config.getSinkConnectorSchemaMessageMode();
        this.schemaClass = mode == SinkConnectorSchemaMessageMode.LOG_MESSAGE
                ? config.getSinkConnectorSchemaProtoMessageClass() : config.getSinkConnectorSchemaProtoKeyClass();
    }


    public BigQueryPayload convert(List<Message> messages) {
        ProtoRows.Builder rowBuilder = ProtoRows.newBuilder();
        BigQueryPayload payload = new BigQueryPayload();
        Descriptors.Descriptor descriptor = writer.getDescriptor();
        long validIndex = 0;
        for (int index = 0; index < messages.size(); index++) {
            Message message = messages.get(index);
            try {
                DynamicMessage convertedMessage = convert(message, descriptor);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, null, true);
                payload.addMetadataRecord(metadata);
                payload.putValidIndexToInputIndex(validIndex++, index);
                rowBuilder.addSerializedRows(convertedMessage.toByteString());
            } catch (UnknownFieldsException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.UNKNOWN_FIELDS_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (EmptyMessageException | UnsupportedOperationException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.INVALID_MESSAGE_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (DeserializerException | IllegalArgumentException | IOException e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.DESERIALIZATION_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            } catch (Exception e) {
                ErrorInfo errorInfo = new ErrorInfo(e, ErrorType.SINK_UNKNOWN_ERROR);
                BigQueryRecordMeta metadata = new BigQueryRecordMeta(index, errorInfo, false);
                payload.addMetadataRecord(metadata);
            }
        }
        payload.setPayload(rowBuilder.build());
        return payload;
    }

    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload payload) throws ExecutionException, InterruptedException {
        return writer.appendAndGet(payload);
    }


    private DynamicMessage convert(Message message, Descriptors.Descriptor descriptor) throws IOException {
        ParsedMessage parsedMessage = parser.parse(message, mode, schemaClass);
        parsedMessage.validate(config);
        DynamicMessage.Builder messageBuilder = convert(parsedMessage, descriptor, true);
        BigQueryProtoUtils.addMetadata(message.getMetadata(), messageBuilder, descriptor, config);
        return messageBuilder.build();
    }

    private Object getFieldValue(Descriptors.FieldDescriptor outputField, SchemaField field, Object value, boolean isTopLevel) {
        // float values converted as java double type
        if (field.getType().equals(SchemaFieldType.FLOAT) || field.getType().equals(SchemaFieldType.DOUBLE)) {
            double val = Double.parseDouble(value.toString());
            boolean valid = !Double.isInfinite(val) && !Double.isNaN(val);
            if (!valid) {
                throw new IllegalArgumentException(String.format("Float/Double value is not valid for field \"%s\"", outputField.getFullName()));
            }
            return val;
        }
        // all integer value types in protobuf converted as long
        if (field.getType().equals(SchemaFieldType.INT) || field.getType().equals(SchemaFieldType.LONG)) {
            return Long.valueOf(value.toString());
        }
        if (field.getType().equals(SchemaFieldType.MESSAGE)) {
            ParsedMessage msg = (ParsedMessage) value;
            if (msg.getSchema().logicalType().equals(LogicalType.TIMESTAMP)) {
                return TimeStampUtils.getBQInstant(msg.getLogicalValue().getTimestamp(), outputField, isTopLevel, config);
            }
            if (msg.getSchema().logicalType().equals(LogicalType.STRUCT)) {
                JSONObject json = new JSONObject(msg.getLogicalValue().getStruct());
                return json.toString();
            }
            return convert(msg, outputField.getMessageType(), false).build();
        }
        return value;
    }

    private Object getListValue(Descriptors.FieldDescriptor outputField, SchemaField schemaField, List<?> value) {
        return value
                .stream()
                .map(eachValue -> getFieldValue(outputField, schemaField, eachValue, false))
                .collect(Collectors.toList());
    }

    private DynamicMessage.Builder convert(ParsedMessage inputMessage, Descriptors.Descriptor descriptor, boolean isTopLevel) {
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        Map<SchemaField, Object> fields = inputMessage.getFields();
        for (Map.Entry<SchemaField, Object> inputField : fields.entrySet()) {
            SchemaField schemaField = inputField.getKey();
            Descriptors.FieldDescriptor outputField = descriptor.findFieldByName(schemaField.getName().toLowerCase());
            if (outputField == null) {
                continue;
            }
            Object value = inputField.getValue();
            if (schemaField.isRepeated()) {
                messageBuilder.setField(outputField, getListValue(outputField, schemaField, (List<?>) value));
                continue;
            }
            messageBuilder.setField(outputField, getFieldValue(outputField, schemaField, value, isTopLevel));
        }
        return messageBuilder;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}

