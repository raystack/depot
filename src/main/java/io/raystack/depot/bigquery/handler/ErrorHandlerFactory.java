package org.raystack.depot.bigquery.handler;

import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.config.enums.SinkConnectorSchemaDataType;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;

public class ErrorHandlerFactory {
    public static ErrorHandler create(BigQuerySinkConfig sinkConfig, BigQueryClient bigQueryClient,
            StatsDReporter statsDReprter) {
        if (SinkConnectorSchemaDataType.JSON == sinkConfig.getSinkConnectorSchemaDataType()) {
            return new JsonErrorHandler(
                    bigQueryClient,
                    sinkConfig, new Instrumentation(statsDReprter, JsonErrorHandler.class));
        }
        return new NoopErrorHandler();
    }
}
