package com.gotocompany.depot.bigquery;

import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.json.BigqueryJsonUpdateListener;
import com.gotocompany.depot.bigquery.proto.BigqueryProtoUpdateListener;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;

public class BigqueryStencilUpdateListenerFactory {
    public static DepotStencilUpdateListener create(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache converterCache, StatsDReporter statsDReporter) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new BigqueryJsonUpdateListener(config, converterCache, bqClient, new Instrumentation(statsDReporter, BigqueryJsonUpdateListener.class));
            case PROTOBUF:
                return new BigqueryProtoUpdateListener(config, bqClient, converterCache);
            default:
                throw new ConfigurationException("Schema Type is not supported");
        }
    }
}
