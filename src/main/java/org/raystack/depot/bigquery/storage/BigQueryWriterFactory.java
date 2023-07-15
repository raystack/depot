package org.raystack.depot.bigquery.storage;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import org.raystack.depot.bigquery.storage.proto.BigQueryProtoWriter;
import org.raystack.depot.bigquery.storage.json.BigQueryJsonWriter;
import org.raystack.depot.common.Function3;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;

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
                return new BigQueryProtoWriter(config, bqWriterCreator, credCreator, streamCreator, instrumentation,
                        metrics);
            case JSON:
                return new BigQueryJsonWriter(config);
            default:
                throw new IllegalArgumentException("Couldn't initialise the BQ writer");
        }
    }
}
