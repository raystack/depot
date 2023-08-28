package com.gotocompany.depot.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.gotocompany.depot.exception.DeserializerException;
import com.jayway.jsonpath.Configuration;
import com.google.protobuf.Message;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.LogicalValue;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.schema.Schema;
import com.gotocompany.depot.schema.SchemaField;
import com.gotocompany.depot.schema.proto.ProtoSchema;
import com.gotocompany.depot.schema.proto.ProtoSchemaField;
import com.gotocompany.depot.utils.ProtoUtils;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class ProtoParsedMessage implements ParsedMessage {


    private final Message dynamicMessage;
    private final Configuration jsonPathConfig;

    public ProtoParsedMessage(DynamicMessage dynamicMessage, Configuration jsonPathConfig) {
        this.dynamicMessage = dynamicMessage;
        this.jsonPathConfig = jsonPathConfig;
    }

    public ProtoParsedMessage(Message dynamicMessage, Configuration jsonPathConfig) {
        this.dynamicMessage = dynamicMessage;
        this.jsonPathConfig = jsonPathConfig;

    }

    public String toString() {
        return dynamicMessage.toString();
    }

    @Override
    public Object getRaw() {
        return dynamicMessage;
    }

    @Override
    public JSONObject toJson() {
        String json;
        try {
            json = JsonFormat.printer().print(dynamicMessage);
        } catch (InvalidProtocolBufferException | IllegalArgumentException e) {
            throw new DeserializerException(e.getMessage());
        }
        return new JSONObject(json);
    }

    @Override
    public void validate(SinkConfig config) {
        if (!config.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
            log.error("Unknown fields {}", UnknownProtoFields.toString(dynamicMessage.toByteArray()));
            throw new UnknownFieldsException(dynamicMessage);
        }
    }

    private Object getProtoValue(Descriptors.FieldDescriptor fd, Object value) {
        switch (fd.getJavaType()) {
            case ENUM:
                return value.toString();
            case MESSAGE:
                return new ProtoParsedMessage((Message) value, jsonPathConfig);
            default:
                return value;
        }
    }

    @Override
    public Map<SchemaField, Object> getFields() {
        return dynamicMessage.getDescriptorForType().getFields().stream().filter(fd -> {
            Object value = dynamicMessage.getField(fd);
            if (value == null) {
                return false;
            }
            if (fd.isRepeated()) {
                return !((List<?>) value).isEmpty();
            }
            return !value.toString().isEmpty();
        }).collect(Collectors.toMap(ProtoSchemaField::new, fd -> {
            Object value = dynamicMessage.getField(fd);
            if (fd.isRepeated()) {
                return ((List<?>) value).stream().map(v -> getProtoValue(fd, v)).collect(Collectors.toList());
            }
            return getProtoValue(fd, value);
        }));
    }

    public Object getFieldByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, dynamicMessage, jsonPathConfig);
    }

    @Override
    public Schema getSchema() {
        return new ProtoSchema(dynamicMessage.getDescriptorForType());
    }

    @Override
    public LogicalValue getLogicalValue() {
        return new ProtoLogicalValue(dynamicMessage, getSchema());
    }
}
