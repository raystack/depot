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
        this(protoField, null);
    }

    public ProtoOdpfMessageSchema(ProtoField protoField, Properties properties) {
        this.protoField = protoField;
        this.properties = properties;
    }

    @Override
    public Properties getSchema() {
        return this.properties;
    }
}
