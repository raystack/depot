package io.odpf.sink.connectors.bigquery.handler;

import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.config.enums.InputSchemaDataType;

public class ErrorHandlerFactory {
    public static ErrorHandler create(BigQuerySinkConfig sinkConfig, BigQueryClient bigQueryClient) {
        if (InputSchemaDataType.JSON == sinkConfig.getSinkConnectorSchemaDataTye()) {
            return new JsonErrorHandler(
                    bigQueryClient,
                    sinkConfig.getTablePartitionKey(),
                    sinkConfig.getOutputDefaultDatatypeStringEnable());
        }
        return new NoopErrorHandler();
    }
}
