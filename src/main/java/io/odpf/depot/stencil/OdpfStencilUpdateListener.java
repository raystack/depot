package org.raystack.depot.stencil;

import com.google.protobuf.Descriptors;
import org.raystack.depot.message.OdpfMessageParser;
import org.raystack.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public abstract class OdpfStencilUpdateListener implements SchemaUpdateListener {
    @Getter
    @Setter
    private OdpfMessageParser raystackMessageParser;

    public void onSchemaUpdate(final Map<String, Descriptors.Descriptor> newDescriptor) {
        // default implementation is empty
    }

    public abstract void updateSchema();
}
