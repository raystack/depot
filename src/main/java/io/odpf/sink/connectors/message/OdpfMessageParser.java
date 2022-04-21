package io.odpf.sink.connectors.message;

import java.io.IOException;

public interface OdpfMessageParser {
    ParsedOdpfMessage parse(OdpfMessage message, InputSchemaMessageMode type, String schemaClass) throws IOException;
}
