package io.odpf.sink.connectors.bigquery;

import io.odpf.sink.connectors.bigquery.converter.MessageRecordConverterCache;
import io.odpf.sink.connectors.bigquery.handler.BigQueryClient;
import io.odpf.sink.connectors.bigquery.json.BigqueryJsonUpdateListener;
import io.odpf.sink.connectors.bigquery.proto.BigqueryProtoUpdateListener;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.stencil.OdpfStencilUpdateListener;

public class BigqueryStencilUpdateListenerFactory {
    public static OdpfStencilUpdateListener getBigqueryStencilUpdateListener(BigQuerySinkConfig config, BigQueryClient bqClient, MessageRecordConverterCache recordConverterCahe) {
        switch (config.getSinkConnectorSchemaDataTye()) {
            case JSON:
                return new BigqueryJsonUpdateListener();
            case PROTOBUF:
                return new BigqueryProtoUpdateListener(config, bqClient, recordConverterCahe);
            default:
                throw new IllegalArgumentException("Schema Type is not supported");
        }
    }
}
