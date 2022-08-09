package io.odpf.depot.message.proto;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.odpf.depot.message.OdpfMessageSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.Properties;

@EqualsAndHashCode
public class ProtoOdpfMessageSchema implements OdpfMessageSchema {

    @Getter
    private final ProtoField protoField;
    private static final Gson GSON = new Gson();
    private final Properties properties;

    public ProtoOdpfMessageSchema(ProtoField protoField) throws IOException {
        this(protoField, createProperties(protoField));
    }

    public ProtoOdpfMessageSchema(ProtoField protoField, Properties properties) throws IOException {
        this.protoField = protoField;
        this.properties = properties;
    }

    @Override
    public Properties getSchema() throws IOException {
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
