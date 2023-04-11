package com.gotocompany.depot.message.proto;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import com.gotocompany.depot.exception.DeserializerException;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.schema.LogicalType;
import com.gotocompany.depot.schema.Schema;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoLogicalValue implements LogicalValue {
    private final Message message;
    private final Schema schema;

    public ProtoLogicalValue(Message message, Schema schema) {
        this.message = message;
        this.schema = schema;
    }

    @Override
    public LogicalType getType() {
        return schema.logicalType();
    }

    @Override
    public Instant getTimestamp() {
        try {
            Timestamp timestamp = Timestamp.parseFrom(message.toByteArray());
            return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Timestamp"), e);
        }
    }

    private Object getValue(Value value) {
        switch (value.getKindCase()) {
            case BOOL_VALUE:
                return value.getBoolValue();
            case NUMBER_VALUE:
                return value.getNumberValue();
            case STRING_VALUE:
                return value.getStringValue();
            case STRUCT_VALUE:
                return getStructValue(value.getStructValue());
            case LIST_VALUE:
                return value.getListValue().getValuesList().stream().map(this::getValue).collect(Collectors.toList());
            default:
                return JSONObject.NULL;
        }
    }

    private Map<String, Object> getStructValue(Struct s) {
        Map<String, Value> fieldsMap = s.getFieldsMap();
        return fieldsMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> getValue(e.getValue())));
    }

    @Override
    public Map<String, Object> getStruct() {
        try {
            Struct s = Struct.parseFrom(message.toByteArray());
            return getStructValue(s);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Struct"), e);
        }
    }

    @Override
    public Duration getDuration() {
        try {
            com.google.protobuf.Duration duration = com.google.protobuf.Duration.parseFrom(message.toByteArray());
            return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializerException(getErrMessage("Duration"), e);
        }
    }

    private String getErrMessage(String type) {
        return String.format("Failed while deserializing given \"%s\" to %s", message.getDescriptorForType().getFullName(), type);
    }
}
