package com.gotocompany.depot.message;

import java.io.IOException;

public class SchemaContainer {

    private final MessageParser parser;
    private MessageSchema schemaKey = null;
    private MessageSchema schemaMessage = null;

    public SchemaContainer(MessageParser parser) {
        this.parser = parser;
    }

    public MessageSchema getSchemaKey(String schemaProtoKeyClass) throws IOException {
        if (schemaKey == null) {
            schemaKey = parser.getSchema(schemaProtoKeyClass);
        }
        return schemaKey;
    }

    public MessageSchema getSchemaMessage(String schemaProtoMessageClass) throws IOException {
        if (schemaMessage == null) {
            schemaMessage = parser.getSchema(schemaProtoMessageClass);
        }
        return schemaMessage;
    }
}
