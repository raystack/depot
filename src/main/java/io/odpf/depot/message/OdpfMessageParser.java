package io.odpf.depot.message;

import java.io.IOException;

public interface OdpfMessageParser {
    ParsedOdpfMessage parse(OdpfMessage message, InputSchemaMessageMode type, String schemaClass) throws IOException;

    OdpfMessageSchema getSchema(String schemaClass) throws IOException;
}
