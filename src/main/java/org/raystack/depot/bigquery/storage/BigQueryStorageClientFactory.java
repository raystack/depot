package org.raystack.depot.bigquery.storage;

import org.raystack.depot.bigquery.storage.proto.BigQueryProtoStorageClient;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.message.MessageParser;

public class BigQueryStorageClientFactory {
    public static BigQueryStorageClient createBigQueryStorageClient(
            BigQuerySinkConfig config,
            MessageParser parser,
            BigQueryWriter bigQueryWriter) {
        switch (config.getSinkConnectorSchemaDataType()) {
            case PROTOBUF:
                return new BigQueryProtoStorageClient(bigQueryWriter, config, parser);
            default:
                throw new IllegalArgumentException("Invalid data type");
        }
    }
}
