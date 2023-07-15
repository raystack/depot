package org.raystack.depot.message;

import java.io.IOException;

public interface MessageParser {
    ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass)
            throws IOException;

    MessageSchema getSchema(String schemaClass) throws IOException;
}
