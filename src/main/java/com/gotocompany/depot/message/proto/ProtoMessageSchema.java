package com.gotocompany.depot.message.proto;

import com.google.gson.Gson;
import com.gotocompany.depot.message.MessageSchema;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.io.IOException;
import java.util.Properties;

@EqualsAndHashCode
public class ProtoMessageSchema implements MessageSchema {

    @Getter
    private final ProtoField protoField;
    private static final Gson GSON = new Gson();
    private final Properties properties;

    public ProtoMessageSchema(ProtoField protoField) throws IOException {
        this(protoField, null);
    }

    public ProtoMessageSchema(ProtoField protoField, Properties properties) {
        this.protoField = protoField;
        this.properties = properties;
    }

    @Override
    public Properties getSchema() {
        return this.properties;
    }
}
