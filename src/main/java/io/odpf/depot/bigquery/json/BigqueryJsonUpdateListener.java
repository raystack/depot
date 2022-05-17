package io.odpf.depot.bigquery.json;

import io.odpf.depot.bigquery.handler.MessageRecordConverter;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;

public class BigqueryJsonUpdateListener extends OdpfStencilUpdateListener {
    private final MessageRecordConverterCache converterCache;
    private BigQuerySinkConfig config;

    public BigqueryJsonUpdateListener(BigQuerySinkConfig config, MessageRecordConverterCache converterCache) {
        this.converterCache = converterCache;
        this.config = config;
    }

    @Override
    public void updateSchema() {
        OdpfMessageParser parser = getOdpfMessageParser();
        MessageRecordConverter messageRecordConverter = new MessageRecordConverter(parser, config, null);
        converterCache.setMessageRecordConverter(messageRecordConverter);
    }
}
