package io.odpf.sink.connectors.message.proto;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.sink.connectors.common.Tuple;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.expcetion.UnknownFieldsException;
import io.odpf.sink.connectors.expcetion.ConfigurationException;
import io.odpf.sink.connectors.message.OdpfMessageSchema;
import io.odpf.sink.connectors.message.ParsedOdpfMessage;
import io.odpf.sink.connectors.message.proto.converter.fields.NestedProtoField;
import io.odpf.sink.connectors.message.proto.converter.fields.ProtoField;
import io.odpf.sink.connectors.message.proto.converter.fields.ProtoFieldFactory;
import io.odpf.sink.connectors.utils.ProtoUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class ProtoOdpfParsedMessage implements ParsedOdpfMessage {
    private final DynamicMessage dynamicMessage;

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
    public Map<String, Object> getMapping(OdpfMessageSchema schema) throws IOException {
        if (schema.getSchema() == null) {
            throw new ConfigurationException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        return getMappings(dynamicMessage, (Properties) schema.getSchema());
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMappings(DynamicMessage message, Properties columnMapping) {
        if (message == null || columnMapping == null || columnMapping.isEmpty()) {
            return new HashMap<>();
        }
        Descriptors.Descriptor descriptorForType = message.getDescriptorForType();

        Map<String, Object> row = new HashMap<>(columnMapping.size());
        columnMapping.forEach((key, value) -> {
            String columnName = value.toString();
            String columnIndex = key.toString();
            if (columnIndex.equals(Constants.Config.RECORD_NAME)) {
                return;
            }
            int protoIndex = Integer.parseInt(columnIndex);
            Descriptors.FieldDescriptor fieldDesc = descriptorForType.findFieldByNumber(protoIndex);
            if (fieldDesc != null && !message.getField(fieldDesc).toString().isEmpty()) {
                Object field = message.getField(fieldDesc);
                ProtoField protoField = ProtoFieldFactory.getField(fieldDesc, field);
                Object fieldValue = protoField.getValue();

                if (fieldValue instanceof List) {
                    addRepeatedFields(row, value, (List<Object>) fieldValue);
                    return;
                }
                if (protoField.getClass().getName().equals(NestedProtoField.class.getName())) {
                    Tuple<String, Object> nestedColumns = getNestedColumnName(field, value);
                    row.put(nestedColumns.getFirst(), nestedColumns.getSecond());
                } else {
                    row.put(columnName, fieldValue);
                }
            }
        });
        return row;
    }

    private Tuple<String, Object> getNestedColumnName(Object field, Object value) {
        try {
            String columnName = getNestedColumnName((Properties) value);
            Object fieldValue = getMappings((DynamicMessage) field, (Properties) value);
            return new Tuple<>(columnName, fieldValue);
        } catch (Exception e) {
            log.error("Exception::Handling nested field failure: {}", e.getMessage());
            throw e;
        }
    }

    private String getNestedColumnName(Properties value) {
        return value.get(Constants.Config.RECORD_NAME).toString();
    }

    private void addRepeatedFields(Map<String, Object> row, Object value, List<Object> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        List<Object> repeatedNestedFields = new ArrayList<>();
        String columnName = null;
        for (Object f : fieldValue) {
            if (f instanceof DynamicMessage) {
                assert value instanceof Properties;
                Properties nestedMappings = (Properties) value;
                repeatedNestedFields.add(getMappings((DynamicMessage) f, nestedMappings));
                columnName = getNestedColumnName(nestedMappings);
            } else {
                repeatedNestedFields.add(f);
                assert value instanceof String;
                columnName = (String) value;
            }
        }
        row.put(columnName, repeatedNestedFields);
    }
}
