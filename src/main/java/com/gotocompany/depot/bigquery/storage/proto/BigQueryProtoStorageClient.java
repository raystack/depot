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
import com.gotocompany.depot.message.proto.converter.fields.DurationProtoField;
import com.gotocompany.depot.message.proto.converter.fields.MessageProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoFieldFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
        DynamicMessage.Builder messageBuilder = convert((DynamicMessage) parsedMessage.getRaw(), descriptor, true);
        BigQueryProtoUtils.addMetadata(message.getMetadata(), messageBuilder, descriptor, config);
        return messageBuilder.build();
    }

    private DynamicMessage.Builder convert(DynamicMessage inputMessage, Descriptors.Descriptor descriptor, boolean isTopLevel) {
        DynamicMessage.Builder messageBuilder = DynamicMessage.newBuilder(descriptor);
        List<Descriptors.FieldDescriptor> allFields = inputMessage.getDescriptorForType().getFields();
        for (Descriptors.FieldDescriptor inputField : allFields) {
            Descriptors.FieldDescriptor outputField = descriptor.findFieldByName(inputField.getName().toLowerCase());
            if (outputField == null) {
                // not found in table
                continue;
            }
            ProtoField protoField = ProtoFieldFactory.getField(inputField, inputMessage.getField(inputField));
            Object fieldValue = protoField.getValue();
            if (fieldValue instanceof List) {
                addRepeatedFields(messageBuilder, outputField, (List<?>) fieldValue);
                continue;
            }
            if (fieldValue.toString().isEmpty()) {
                continue;
            }
            if (fieldValue instanceof Instant) {
                if (((Instant) fieldValue).getEpochSecond() > 0) {
                    long timeStampValue = TimeStampUtils.getBQInstant((Instant) fieldValue, outputField, isTopLevel, config);
                    messageBuilder.setField(outputField, timeStampValue);
                }
            } else if (protoField.getClass().getName().equals(MessageProtoField.class.getName())
                    || protoField.getClass().getName().equals(DurationProtoField.class.getName())) {
                Descriptors.Descriptor messageType = outputField.getMessageType();
                messageBuilder.setField(outputField, convert((DynamicMessage) fieldValue, messageType, false).build());
            } else {
                messageBuilder.setField(outputField, fieldValue);
            }
        }
        return messageBuilder;
    }

    private void addRepeatedFields(DynamicMessage.Builder messageBuilder, Descriptors.FieldDescriptor outputField, List<?> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                Descriptors.Descriptor messageType = outputField.getMessageType();
                repeatedNestedFields.add(convert((DynamicMessage) f, messageType, false).build());
            } else {
                if (f instanceof Instant) {
                    if (((Instant) f).getEpochSecond() > 0) {
                        repeatedNestedFields.add(TimeStampUtils.getBQInstant((Instant) f, outputField, false, config));
                    }
                } else {
                    repeatedNestedFields.add(f);
                }
            }
        }
        messageBuilder.setField(outputField, repeatedNestedFields);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}

