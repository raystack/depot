package com.gotocompany.depot.bigquery.storage;

import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStorageClient;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;

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
