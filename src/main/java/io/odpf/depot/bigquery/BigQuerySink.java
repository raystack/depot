package org.raystack.depot.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.client.BigQueryResponseParser;
import org.raystack.depot.bigquery.client.BigQueryRow;
import org.raystack.depot.bigquery.handler.ErrorHandler;
import org.raystack.depot.bigquery.models.Record;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.message.OdpfMessage;
import org.raystack.depot.OdpfSink;
import org.raystack.depot.OdpfSinkResponse;
import org.raystack.depot.bigquery.models.Records;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BigQuerySink implements OdpfSink {

    private final BigQueryClient bigQueryClient;
    private final BigQueryRow rowCreator;
    private final MessageRecordConverterCache messageRecordConverterCache;
    private final Instrumentation instrumentation;
    private final BigQueryMetrics bigQueryMetrics;
    private final ErrorHandler errorHandler;

    public BigQuerySink(BigQueryClient client,
            MessageRecordConverterCache converterCache,
            BigQueryRow rowCreator,
            BigQueryMetrics bigQueryMetrics,
            Instrumentation instrumentation,
            ErrorHandler errorHandler) {
        this.bigQueryClient = client;
        this.messageRecordConverterCache = converterCache;
        this.rowCreator = rowCreator;
        this.instrumentation = instrumentation;
        this.bigQueryMetrics = bigQueryMetrics;
        this.errorHandler = errorHandler;
    }

    @Override
    public void close() throws IOException {
    }

    private InsertAllResponse insertIntoBQ(List<Record> records) {
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(bigQueryClient.getTableID());
        records.forEach((Record m) -> builder.addRow(rowCreator.of(m)));
        return bigQueryClient.insertAll(builder.build());
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messageList) {
        Records records = messageRecordConverterCache.getMessageRecordConverter().convert(messageList);
        OdpfSinkResponse raystackSinkResponse = new OdpfSinkResponse();
        records.getInvalidRecords().forEach(
                invalidRecord -> raystackSinkResponse.addErrors(invalidRecord.getIndex(),
                        invalidRecord.getErrorInfo()));
        if (records.getValidRecords().size() > 0) {
            InsertAllResponse response = insertIntoBQ(records.getValidRecords());
            instrumentation.logInfo("Pushed a batch of {} records to BQ. Insert success?: {}",
                    records.getValidRecords().size(), !response.hasErrors());
            if (response.hasErrors()) {
                Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser
                        .getErrorsFromBQResponse(records.getValidRecords(), response, bigQueryMetrics, instrumentation);
                errorInfoMap.forEach(raystackSinkResponse::addErrors);
                errorHandler.handle(response.getInsertErrors(), records.getValidRecords());
            }
        }
        return raystackSinkResponse;
    }
}
