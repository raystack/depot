package io.odpf.sink.connectors.bigquery.converter;

import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import io.odpf.sink.connectors.message.InputSchemaMessageMode;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.bigquery.models.Records;
import io.odpf.sink.connectors.bigquery.proto.UnknownProtoFields;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.error.ErrorType;
import io.odpf.sink.connectors.expcetion.DeserializerException;
import io.odpf.sink.connectors.expcetion.EmptyMessageException;
import io.odpf.sink.connectors.expcetion.UnknownFieldsException;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.utils.ProtoUtils;
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
    private final RowMapper rowMapper;
    private final OdpfMessageParser parser;
    private final BigQuerySinkConfig config;

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
        if (message.getLogMessage() == null || message.getLogMessage().length == 0) {
            log.info("empty message found {}", message.getMetadataString());
            throw new EmptyMessageException();
        }
        try {
            DynamicMessage dynamicMessage = (DynamicMessage) parser.parse(message, InputSchemaMessageMode.LOG_MESSAGE, config.getInputSchemaProtoClass()).getRaw();
            if (!config.getInputSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
                log.info("unknown fields found {}", message.getMetadataString());
                throw new UnknownFieldsException(dynamicMessage);
            }
            Map<String, Object> columns = rowMapper.map(dynamicMessage);
            if (config.shouldAddMetadata()) {
                addMetadata(columns, message);
            }
            return new Record(message.getMetadata(), columns, index, null);
        } catch (InvalidProtocolBufferException e) {
            log.error("failed to deserialize message: {}, {} ", UnknownProtoFields.toString(message.getLogMessage()), message.getMetadataString());
            throw new DeserializerException("failed to deserialize ", e);
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
