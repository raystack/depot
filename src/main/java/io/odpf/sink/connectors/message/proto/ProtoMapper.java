package io.odpf.sink.connectors.message.proto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.List;

public class ProtoMapper {

    public static String generateColumnMappings(List<ProtoField> fields) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode objectNode = generateColumnMappingsJson(fields);
        return objectMapper.writeValueAsString(objectNode);
    }

    private static ObjectNode generateColumnMappingsJson(List<ProtoField> fields) {
        if (fields.size() == 0) {
            return JsonNodeFactory.instance.objectNode();
        }

        ObjectNode objNode = JsonNodeFactory.instance.objectNode();
        for (ProtoField field : fields) {
            if (field.isNested()) {
                ObjectNode innerJSONValue = generateColumnMappingsJson(field.getFields());
                innerJSONValue.put(Constants.Config.RECORD_NAME, field.getName());
                objNode.set(String.valueOf(field.getIndex()), innerJSONValue);
            } else {
                objNode.put(String.valueOf(field.getIndex()), field.getName());
            }
        }
        return objNode;
    }
}
