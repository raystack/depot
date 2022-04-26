package io.odpf.sink.connectors.bigquery;

import io.odpf.sink.connectors.bigquery.handler.MessageRecordConverterCache;
import io.odpf.sink.connectors.bigquery.handler.BigQueryClient;
import io.odpf.sink.connectors.bigquery.json.BigqueryJsonUpdateListener;
import io.odpf.sink.connectors.bigquery.proto.BigqueryProtoUpdateListener;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.expcetion.ConfigurationException;
import io.odpf.sink.connectors.stencil.OdpfStencilUpdateListener;

public class BigqueryStencilUpdateListenerFactory {
    public static OdpfStencilUpdateListener create(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache converterCache) {
        switch (config.getSinkConnectorSchemaDataTye()) {
            case JSON:
                return new BigqueryJsonUpdateListener();
            case PROTOBUF:
                return new BigqueryProtoUpdateListener(config, bqClient, converterCache);
            default:
                throw new ConfigurationException("Schema Type is not supported");
        }
    }
}
