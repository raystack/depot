package io.odpf.depot.message;

import java.io.IOException;

public class SchemaContainer {

    private final OdpfMessageParser parser;
    private OdpfMessageSchema schemaKey = null;
    private OdpfMessageSchema schemaMessage = null;

    public SchemaContainer(OdpfMessageParser parser) {
        this.parser = parser;
    }

    public OdpfMessageSchema getSchemaKey(String schemaProtoKeyClass) throws IOException {
        if (schemaKey == null) {
            schemaKey = parser.getSchema(schemaProtoKeyClass);
        }
        return schemaKey;
    }

    public OdpfMessageSchema getSchemaMessage(String schemaProtoMessageClass) throws IOException {
        if (schemaMessage == null) {
            schemaMessage = parser.getSchema(schemaProtoMessageClass);
        }
        return schemaMessage;
    }
}
