package com.gotocompany.depot.stencil;

import com.google.protobuf.Descriptors;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.stencil.SchemaUpdateListener;
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
