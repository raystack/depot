package com.gotocompany.depot.bigquery.storage;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.gotocompany.depot.bigquery.storage.proto.BigQueryProtoStream;
import com.gotocompany.depot.config.BigQuerySinkConfig;

import java.io.FileInputStream;
import java.io.IOException;

public class BigQueryWriterUtils {
    private static final String DEFAULT_STREAM_SUFFIX = "/_default";

    public static BigQueryWriteClient getBigQueryWriterClient(BigQuerySinkConfig config) {
        try {

            BigQueryWriteSettings settings = BigQueryWriteSettings.newBuilder()
                    .setCredentialsProvider(getCredentialsProvider(config))
                    .build();
            return BigQueryWriteClient.create(settings);
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't initialise writer client", e);
        }
    }

    public static CredentialsProvider getCredentialsProvider(BigQuerySinkConfig config) {
        try {
            return FixedCredentialsProvider.create(
                    GoogleCredentials.fromStream(new FileInputStream(config.getBigQueryCredentialPath())));
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't initialise credential provider", e);
        }
    }

    public static BigQueryStream getStreamWriter(BigQuerySinkConfig config, CredentialsProvider credentialsProvider, ProtoSchema schema) {
        try {
            String streamName = getDefaultStreamName(config);
            StreamWriter.Builder builder = StreamWriter.newBuilder(streamName);
            builder.setCredentialsProvider(credentialsProvider);
            builder.setWriterSchema(schema);
            builder.setEnableConnectionPool(false);
            return new BigQueryProtoStream(builder.build());
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't initialise Stream Writer", e);
        }
    }

    public static String getDefaultStreamName(BigQuerySinkConfig config) {
        TableName parentTable = TableName.of(config.getGCloudProjectID(), config.getDatasetName(), config.getTableName());
        return parentTable.toString() + DEFAULT_STREAM_SUFFIX;
    }
}
