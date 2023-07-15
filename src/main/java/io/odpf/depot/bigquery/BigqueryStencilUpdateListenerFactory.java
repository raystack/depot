package org.raystack.depot.bigquery;

import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.bigquery.json.BigqueryJsonUpdateListener;
import org.raystack.depot.bigquery.proto.BigqueryProtoUpdateListener;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.OdpfStencilUpdateListener;

public class BigqueryStencilUpdateListenerFactory {
    public static OdpfStencilUpdateListener create(BigQuerySinkConfig config, BigQueryClient bqClient,
            MessageRecordConverterCache converterCache, StatsDReporter statsDReporter) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case JSON:
                return new BigqueryJsonUpdateListener(config, converterCache, bqClient,
                        new Instrumentation(statsDReporter, BigqueryJsonUpdateListener.class));
            case PROTOBUF:
                return new BigqueryProtoUpdateListener(config, bqClient, converterCache);
            default:
                throw new ConfigurationException("Schema Type is not supported");
        }
    }
}
