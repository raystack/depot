package io.odpf.depot.bigquery.json;

import com.google.protobuf.Descriptors;
import io.odpf.depot.bigquery.handler.MessageRecordConverter;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;

import java.util.Map;

public class BigqueryJsonUpdateListener extends OdpfStencilUpdateListener {
    private final MessageRecordConverterCache converterCache;
    private BigQuerySinkConfig config;

    public BigqueryJsonUpdateListener(BigQuerySinkConfig config, MessageRecordConverterCache converterCache) {
        this.converterCache = converterCache;
        this.config = config;
    }

    @Override
    public void onSchemaUpdate(Map<String, Descriptors.Descriptor> newDescriptor) {

        OdpfMessageParser parser = getOdpfMessageParser();
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(parser, config, null);
        converterCache.setMessageRecordConverter(messageRecordConverter);
    }
}
