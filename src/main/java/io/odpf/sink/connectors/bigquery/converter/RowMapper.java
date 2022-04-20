package io.odpf.sink.connectors.bigquery.converter;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.sink.connectors.bigquery.converter.fields.NestedProtoField;
import io.odpf.sink.connectors.bigquery.converter.fields.ProtoField;
import io.odpf.sink.connectors.bigquery.exception.ConfigurationException;
import io.odpf.sink.connectors.bigquery.models.Constants;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
@AllArgsConstructor
public class RowMapper {

    private final Properties mapping;

    public Map<String, Object> map(DynamicMessage message) {
        if (mapping == null) {
            throw new ConfigurationException("BQ_PROTO_COLUMN_MAPPING is not configured");
        }
        return getMappings(message, mapping);
    }

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
                    try {
                        columnName = getNestedColumnName((Properties) value);
                        fieldValue = getMappings((DynamicMessage) field, (Properties) value);
                    } catch (Exception e) {
                        log.error("Exception::Handling nested field failure: {}", e.getMessage());
                        throw e;
                    }
                }
                row.put(columnName, fieldValue);
            }
        });
        return row;
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
                Properties nestedMappings = (Properties) value;
                repeatedNestedFields.add(getMappings((DynamicMessage) f, nestedMappings));
                columnName = getNestedColumnName(nestedMappings);
            } else {
                repeatedNestedFields.add(f);
                columnName = (String) value;
            }
        }
        row.put(columnName, repeatedNestedFields);
    }
}
