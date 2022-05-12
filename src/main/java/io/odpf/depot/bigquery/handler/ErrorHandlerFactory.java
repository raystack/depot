package io.odpf.depot.bigquery.handler;

import io.odpf.depot.config.enums.InputSchemaDataType;
import io.odpf.depot.config.BigQuerySinkConfig;

public class ErrorHandlerFactory {
    public static ErrorHandler create(BigQuerySinkConfig sinkConfig, BigQueryClient bigQueryClient) {
        if (InputSchemaDataType.JSON == sinkConfig.getSinkConnectorSchemaDataType()) {
            return new JsonErrorHandler(
                    bigQueryClient,
                    sinkConfig);
        }
        return new NoopErrorHandler();
    }
}
