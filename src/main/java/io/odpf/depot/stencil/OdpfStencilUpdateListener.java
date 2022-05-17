package io.odpf.depot.stencil;

import com.google.protobuf.Descriptors;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public abstract class OdpfStencilUpdateListener implements SchemaUpdateListener {
    @Getter
    @Setter
    private OdpfMessageParser odpfMessageParser;

    public void onSchemaUpdate(final Map<String, Descriptors.Descriptor> newDescriptor) {
        // default implementation is empty
    }

    public abstract void updateSchema();
}
