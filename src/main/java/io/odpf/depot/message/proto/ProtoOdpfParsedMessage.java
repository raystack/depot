package io.odpf.depot.message.proto;

import com.google.api.client.util.DateTime;
import com.google.api.client.util.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Timestamp;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.exception.UnknownFieldsException;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.ParsedOdpfMessage;
import io.odpf.depot.message.proto.converter.fields.ProtoField;
import io.odpf.depot.message.proto.converter.fields.ProtoFieldFactory;
import io.odpf.depot.utils.ProtoUtils;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

@Slf4j
public class ProtoOdpfParsedMessage implements ParsedOdpfMessage {
    private final DynamicMessage dynamicMessage;

    private final Map<OdpfMessageSchema, Map<String, Object>> cachedMapping = new HashMap<>();

    public ProtoOdpfParsedMessage(DynamicMessage dynamicMessage) {
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
    public void validate(OdpfSinkConfig config) {
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
        if (fd.getType().equals(Descriptors.FieldDescriptor.Type.MESSAGE) &&
                !fd.getMessageType().getFullName().equals(com.google.protobuf.Struct.getDescriptor().getFullName())) {
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
            if(!allFields.containsKey(field)) {
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

    public Object getFieldByName(String name, OdpfMessageSchema odpfMessageSchema) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Invalid field config : name can not be empty");
        }
        String[] keys = name.split("\\.");
        Object currentValue = dynamicMessage;
        Descriptors.FieldDescriptor descriptor = null;
        for (String key : keys) {
            if (!(currentValue instanceof DynamicMessage)) {
                throw new IllegalArgumentException("Invalid field config : " + name);
            }
            DynamicMessage message = (DynamicMessage) currentValue;
            descriptor = message.getDescriptorForType().findFieldByName(key);
            if (descriptor == null) {
                throw new IllegalArgumentException("Invalid field config : " + name);
            }
            currentValue = message.getField(descriptor);
        }
        return ProtoFieldFactory.getField(descriptor, currentValue);
    }
}
