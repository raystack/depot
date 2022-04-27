package io.odpf.sink.connectors.stencil;

import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

public abstract class OdpfStencilUpdateListener implements SchemaUpdateListener {
    @Getter
    @Setter
    private OdpfMessageParser odpfMessageParser;
}
