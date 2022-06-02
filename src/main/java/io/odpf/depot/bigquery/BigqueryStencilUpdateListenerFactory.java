package io.odpf.depot.bigquery;

import io.odpf.depot.bigquery.handler.BigQueryClient;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.bigquery.json.BigqueryJsonUpdateListener;
import io.odpf.depot.bigquery.proto.BigqueryProtoUpdateListener;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;

public class BigqueryStencilUpdateListenerFactory {
    public static OdpfStencilUpdateListener create(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache converterCache, StatsDReporter statsDReporter) {
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
