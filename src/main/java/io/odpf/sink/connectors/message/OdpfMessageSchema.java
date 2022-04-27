package io.odpf.sink.connectors.message;

import java.io.IOException;

public interface OdpfMessageSchema {

    Object getSchema() throws IOException;
}
