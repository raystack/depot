package com.gotocompany.depot.message;

import java.io.IOException;

public interface MessageParser {
    ParsedMessage parse(Message message, SinkConnectorSchemaMessageMode type, String schemaClass) throws IOException;
}
