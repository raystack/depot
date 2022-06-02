package io.odpf.depot.bigquery.handler;

import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;

public class ErrorHandlerFactory {
    public static ErrorHandler create(BigQuerySinkConfig sinkConfig, BigQueryClient bigQueryClient, StatsDReporter statsDReprter) {
        if (SinkConnectorSchemaDataType.JSON == sinkConfig.getSinkConnectorSchemaDataType()) {
            return new JsonErrorHandler(
                    bigQueryClient,
                    sinkConfig, new Instrumentation(statsDReprter, JsonErrorHandler.class));
        }
        return new NoopErrorHandler();
    }
}
