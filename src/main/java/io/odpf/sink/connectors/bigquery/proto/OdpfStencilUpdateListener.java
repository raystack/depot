package io.odpf.sink.connectors.bigquery.proto;

import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.stencil.SchemaUpdateListener;

public abstract class OdpfStencilUpdateListener implements SchemaUpdateListener {
    protected OdpfMessageParser messageParser;

    public void setMessageParser(OdpfMessageParser messageParser) {
        this.messageParser = messageParser;
    }
}
