package com.gotocompany.depot.bigquery.storage.proto;

import com.google.api.core.ApiFuture;
import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.GetWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.ProtoRows;
import com.google.cloud.bigquery.storage.v1.ProtoSchema;
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter;
import com.google.cloud.bigquery.storage.v1.StreamWriter;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cloud.bigquery.storage.v1.WriteStreamView;
import com.google.protobuf.Descriptors;
import com.gotocompany.depot.bigquery.storage.BigQueryPayload;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser;
import com.gotocompany.depot.bigquery.storage.BigQueryStream;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterUtils;
import com.gotocompany.depot.common.Function3;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import lombok.Getter;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

public class BigQueryProtoWriter implements BigQueryWriter {

    private final BigQuerySinkConfig config;
    private final Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator;
    private final Function<BigQuerySinkConfig, CredentialsProvider> credCreator;
    private final Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator;
    private final Instrumentation instrumentation;
    private final BigQueryMetrics metrics;
    @Getter
    private StreamWriter streamWriter;
    @Getter
    private Descriptors.Descriptor descriptor;
    private boolean isClosed = false;

    public BigQueryProtoWriter(BigQuerySinkConfig config,
                               Function<BigQuerySinkConfig, BigQueryWriteClient> bqWriterCreator,
                               Function<BigQuerySinkConfig, CredentialsProvider> credCreator,
                               Function3<BigQuerySinkConfig, CredentialsProvider, ProtoSchema, BigQueryStream> streamCreator,
                               Instrumentation instrumentation, BigQueryMetrics metrics) {
        this.config = config;
        this.bqWriterCreator = bqWriterCreator;
        this.credCreator = credCreator;
        this.streamCreator = streamCreator;
        this.instrumentation = instrumentation;
        this.metrics = metrics;
    }

    @Override
    public void init() {
        try {
            String streamName = BigQueryWriterUtils.getDefaultStreamName(config);
            GetWriteStreamRequest writeStreamRequest =
                    GetWriteStreamRequest.newBuilder()
                            .setName(streamName)
                            .setView(WriteStreamView.FULL)
                            .build();
            try (BigQueryWriteClient bigQueryInstance = bqWriterCreator.apply(config)) {
                // This WriteStream is to get the schema of the table.
                WriteStream writeStream = bigQueryInstance.getWriteStream(writeStreamRequest);
                // saving the descriptor for conversion
                descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(writeStream.getTableSchema());
                Instant start = Instant.now();
                BigQueryStream bigQueryStream = streamCreator.apply(config,
                        credCreator.apply(config),
                        ProtoSchemaConverter.convert(descriptor));
                instrument(start, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CREATED);
                assert (bigQueryStream instanceof BigQueryProtoStream);
                // Actual object to write data.
                streamWriter = ((BigQueryProtoStream) bigQueryStream).getStreamWriter();
            }
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Could not initialise the bigquery writer", e);
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            isClosed = true;
            instrumentation.logInfo("Closing StreamWriter");
            Instant start = Instant.now();
            this.streamWriter.close();
            instrument(start, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CLOSED);
        }
    }

    // In the callback one can have the container and set the errors and/or log the response errors
    @Override
    public AppendRowsResponse appendAndGet(BigQueryPayload rows) throws ExecutionException, InterruptedException {
        ApiFuture<AppendRowsResponse> future;
        ProtoRows payload = (ProtoRows) rows.getPayload();
        Instant start;
        if (isClosed) {
            instrumentation.logError("The client is permanently closed. More tasks can not be added");
            return BigQueryStorageResponseParser.get4xxErrorResponse();
        }
        // need to synchronize
        synchronized (this) {
            TableSchema updatedSchema = this.streamWriter.getUpdatedSchema();
            if (updatedSchema != null) {
                instrumentation.logInfo("Updated table schema detected, recreating stream writer");
                try {
                    // Close the StreamWriter
                    start = Instant.now();
                    this.streamWriter.close();
                    instrument(start, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CLOSED);

                    this.descriptor = BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(updatedSchema);

                    start = Instant.now();
                    BigQueryStream bigQueryStream = streamCreator.apply(config,
                            credCreator.apply(config),
                            ProtoSchemaConverter.convert(descriptor));
                    instrument(start, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_CREATED);

                    assert (bigQueryStream instanceof BigQueryProtoStream);
                    // Recreate stream writer
                    streamWriter = ((BigQueryProtoStream) bigQueryStream).getStreamWriter();
                } catch (Descriptors.DescriptorValidationException e) {
                    throw new IllegalArgumentException("Could not initialise the bigquery writer", e);
                }
            }
            // timer for append latency
            start = Instant.now();
            future = streamWriter.append(payload);
        }
        AppendRowsResponse appendRowsResponse = future.get();
        instrument(start, BigQueryMetrics.BigQueryStorageAPIType.STREAM_WRITER_APPEND);
        captureSizeMetric(payload);
        return appendRowsResponse;
    }

    private void captureSizeMetric(ProtoRows payload) {
        instrumentation.captureCount(
                metrics.getBigqueryPayloadSizeMetrics(),
                (long) payload.getSerializedSize(),
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName()),
                String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID()));
    }

    private void instrument(Instant start, BigQueryMetrics.BigQueryStorageAPIType type) {
        instrumentation.incrementCounter(
                metrics.getBigqueryOperationTotalMetric(),
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName()),
                String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID()),
                String.format(BigQueryMetrics.BIGQUERY_API_TAG, type));
        instrumentation.captureDurationSince(
                metrics.getBigqueryOperationLatencyMetric(),
                start,
                String.format(BigQueryMetrics.BIGQUERY_TABLE_TAG, config.getTableName()),
                String.format(BigQueryMetrics.BIGQUERY_DATASET_TAG, config.getDatasetName()),
                String.format(BigQueryMetrics.BIGQUERY_PROJECT_TAG, config.getGCloudProjectID()),
                String.format(BigQueryMetrics.BIGQUERY_API_TAG, type));
    }
}
