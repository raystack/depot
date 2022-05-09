package io.odpf.depot.stencil;

import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

public abstract class OdpfStencilUpdateListener implements SchemaUpdateListener {
    @Getter
    @Setter
    private OdpfMessageParser odpfMessageParser;
}
