package com.gotocompany.depot.message.proto;

import com.google.api.client.util.DateTime;
import com.google.api.client.util.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import com.jayway.jsonpath.Configuration;
import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.exception.UnknownFieldsException;
import com.gotocompany.depot.message.MessageUtils;
import com.gotocompany.depot.message.ParsedMessage;
import com.gotocompany.depot.message.proto.converter.fields.ProtoField;
import com.gotocompany.depot.message.proto.converter.fields.ProtoFieldFactory;
import com.gotocompany.depot.utils.ProtoUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class ProtoParsedMessage implements ParsedMessage {

    private static final Configuration JSON_PATH_CONFIG = Configuration.builder()
            .jsonProvider(new ProtoJsonProvider())
            .build();
    private final DynamicMessage dynamicMessage;

    public ProtoParsedMessage(DynamicMessage dynamicMessage) {
        this.dynamicMessage = dynamicMessage;
    }

    public String toString() {
        return dynamicMessage.toString();
    }

    @Override
    public Object getRaw() {
        return dynamicMessage;
    }

    @Override
    public void validate(SinkConfig config) {
        if (!config.getSinkConnectorSchemaProtoAllowUnknownFieldsEnable() && ProtoUtils.hasUnknownField(dynamicMessage)) {
            log.error("Unknown fields {}", UnknownProtoFields.toString(dynamicMessage.toByteArray()));
            throw new UnknownFieldsException(dynamicMessage);
        }
    }

    @Override
    public Map<String, Object> getMapping() {
        return getMappings(dynamicMessage);
    }

    private Object getFieldValue(Descriptors.FieldDescriptor fd, Object value) {
        if (fd.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.FLOAT)) {
            floatCheck(value);
        }
        if (fd.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE)
                && !fd.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName())) {
            if (fd.getMessageType().getFullName().equals(Timestamp.getDescriptor().getFullName())) {
                ProtoField field = ProtoFieldFactory.getField(fd, value);
                return new DateTime(((Instant) field.getValue()).toEpochMilli());
            }
            return getMappings((DynamicMessage) value);
        }
        ProtoField field = ProtoFieldFactory.getField(fd, value);
        return field.getValue();
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

    private Map<String, Object> getMappings(DynamicMessage message) {
        if (message == null) {
            return new HashMap<>();
        }
        Map<Descriptors.FieldDescriptor, Object> allFields = new TreeMap<>(message.getAllFields());
        for (Descriptors.FieldDescriptor field : message.getDescriptorForType().getFields()) {
            if (!field.getJavaType().equals(Descriptors.FieldDescriptor.JavaType.ENUM)) {
                continue;
            }
            if (!allFields.containsKey(field)) {
                allFields.put(field, message.getField(field));
            }
        }
        return allFields.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().getName(), e -> {
            Object value = e.getValue();
            Descriptors.FieldDescriptor fd = e.getKey();
            if (fd.isRepeated()) {
                List<Object> listValue = (List<Object>) value;
                return listValue.stream().map(o -> getFieldValue(fd, o)).collect(Collectors.toList());
            }
            return getFieldValue(fd, value);
        }));
    }

    public Object getFieldByName(String name) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        return MessageUtils.getFieldFromJsonObject(name, dynamicMessage, JSON_PATH_CONFIG);
    }
}
