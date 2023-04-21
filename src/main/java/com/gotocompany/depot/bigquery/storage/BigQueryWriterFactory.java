package com.gotocompany.depot.bigquery.storage;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoWriter;
import com.gotocompany.depot.bigquery.storage.json.BigQueryJsonWriter;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.util.function.Function;


public class BigQueryWriterFactory {

    public static BigQueryWriter createBigQueryWriter(
            BigQuerySinkConfig config,
            Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
            Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
            Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator,
            Instrumentation instrumentation,
            BigQueryMetrics metrics) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case PROTOBUF:
                return new BigQueryProtoWriter(config, bqWriterCreator, credCreator, streamCreator, instrumentation, metrics);
            case JSON:
                return new BigQueryJsonWriter(config);
            default:
                throw new IllegalArgumentException("Couldn't initialise the BQ writer");
        }
    }
}
