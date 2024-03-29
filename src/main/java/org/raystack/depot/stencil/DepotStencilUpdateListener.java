package org.raystack.depot.stencil;

import com.google.protobuf.Descriptors;
import org.raystack.depot.message.MessageParser;
import org.raystack.stencil.SchemaUpdateListener;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

public abstract class DepotStencilUpdateListener implements SchemaUpdateListener {
    @Getter
    @Setter
    private MessageParser messageParser;

    public void onSchemaUpdate(final Map<String, Descriptors.Descriptor> newDescriptor) {
        // default implementation is empty
    }

    public abstract void updateSchema();
}
