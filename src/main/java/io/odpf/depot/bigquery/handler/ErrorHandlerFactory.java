package io.odpf.depot.bigquery.handler;

import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.config.BigQuerySinkConfig;

public class ErrorHandlerFactory {
    public static ErrorHandler create(BigQuerySinkConfig sinkConfig, BigQueryClient bigQueryClient) {
        if (SinkConnectorSchemaDataType.JSON == sinkConfig.getSinkConnectorSchemaDataType()) {
            return new JsonErrorHandler(
                    bigQueryClient,
                    sinkConfig);
        }
        return new NoopErrorHandler();
    }
}
