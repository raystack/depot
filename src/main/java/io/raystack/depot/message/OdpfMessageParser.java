package org.raystack.depot.message;

import java.io.IOException;

public interface RaystackMessageParser {
    ParsedRaystackMessage parse(RaystackMessage message, SinkConnectorSchemaMessageMode type, String schemaClass)
            throws IOException;

    RaystackMessageSchema getSchema(String schemaClass) throws IOException;
}
