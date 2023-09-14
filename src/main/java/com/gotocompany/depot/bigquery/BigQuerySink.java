package com.gotocompany.depot.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.client.BigQueryResponseParser;
import com.gotocompany.depot.bigquery.client.BigQueryRow;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.handler.ErrorHandler;
import com.gotocompany.depot.bigquery.models.Record;
import com.gotocompany.depot.bigquery.models.Records;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.SinkResponse;
import com.gotocompany.depot.error.ErrorInfo;
import com.gotocompany.depot.message.Message;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BigQuerySink implements Sink {

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
    public SinkResponse pushToSink(List<Message> messageList) {
        Records records = messageRecordConverterCache.getMessageRecordConverter().convert(messageList);
        SinkResponse sinkResponse = new SinkResponse();
        records.getInvalidRecords().forEach(invalidRecord -> sinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (!records.getValidRecords().isEmpty()) {
            InsertAllResponse response = insertIntoBQ(records.getValidRecords());
            instrumentation.logInfo("Pushed a batch of {} records to BQ. Insert success?: {}", records.getValidRecords().size(), !response.hasErrors());
            if (response.hasErrors()) {
                Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser.getErrorsFromBQResponse(records.getValidRecords(), response, bigQueryMetrics, instrumentation);
                errorInfoMap.forEach(sinkResponse::addErrors);
                errorHandler.handle(response.getInsertErrors(), records.getValidRecords());
            }
        }
        return sinkResponse;
    }
}
