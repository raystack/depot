package org.raystack.depot.stencil;

import com.google.protobuf.Descriptors;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public abstract class RaystackStencilUpdateListener implements SchemaUpdateListener {
    @Getter
    @Setter
    private RaystackMessageParser raystackMessageParser;

    public void onSchemaUpdate(final Map<String, Descriptors.Descriptor> newDescriptor) {
        // default implementation is empty
    }

    public abstract void updateSchema();
}
