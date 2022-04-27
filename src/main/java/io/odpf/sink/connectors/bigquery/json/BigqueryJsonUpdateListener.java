package io.odpf.sink.connectors.bigquery.json;

import com.google.protobuf.Descriptors;
import io.odpf.sink.connectors.stencil.OdpfStencilUpdateListener;

import java.util.Map;

public class BigqueryJsonUpdateListener extends OdpfStencilUpdateListener {
    @Override
    public void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptor) {

    }
}
