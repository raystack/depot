package com.gotocompany.depot.bigquery.handler;

import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.config.enums.SinkConnectorSchemaDataType;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.bigquery.client.BigQueryClient;

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
