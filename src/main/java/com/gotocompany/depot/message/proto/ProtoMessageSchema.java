package com.gotocompany.depot.message.proto;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.gotocompany.depot.message.MessageSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;

@EqualsAndHashCode
public class ProtoMessageSchema implements MessageSchema {

    @Getter
    private final ProtoField protoField;
    private static final Gson GSON = new Gson();
    private final Properties properties;

    public ProtoMessageSchema(ProtoField protoField) throws IOException {
        this(protoField, createProperties(protoField));
    }

    public ProtoMessageSchema(ProtoField protoField, Properties properties) {
        this.protoField = protoField;
        this.properties = properties;
    }

    @Override
    public Properties getSchema() {
        return this.properties;
    }

    private static Properties createProperties(ProtoField protoField) throws IOException {
        String protoMappingString = ProtoMapper.generateColumnMappings(protoField.getFields());
        Type type = new TypeToken<Map<String, Object>>() {
        }.getType();
        Map<String, Object> m = GSON.fromJson(protoMappingString, type);
        return mapToProperties(m);
    }

    private static Properties mapToProperties(Map<String, Object> inputMap) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> kv : inputMap.entrySet()) {
            if (kv.getValue() instanceof String) {
                properties.put(kv.getKey(), kv.getValue());
            } else if (kv.getValue() instanceof Map) {
                properties.put(kv.getKey(), mapToProperties((Map) kv.getValue()));
            }
        }
        return properties;
    }
}
