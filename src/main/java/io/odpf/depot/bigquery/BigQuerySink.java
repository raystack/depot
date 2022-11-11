package io.odpf.depot.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import io.odpf.depot.bigquery.client.BigQueryClient;
import io.odpf.depot.bigquery.client.BigQueryResponseParser;
import io.odpf.depot.bigquery.client.BigQueryRow;
import io.odpf.depot.bigquery.handler.ErrorHandler;
import io.odpf.depot.bigquery.models.Record;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.metrics.BigQueryMetrics;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.bigquery.converter.MessageRecordConverterCache;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.bigquery.models.Records;

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
        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        records.getInvalidRecords().forEach(invalidRecord -> odpfSinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));
        if (records.getValidRecords().size() > 0) {
            InsertAllResponse response = insertIntoBQ(records.getValidRecords());
            instrumentation.logInfo("Pushed a batch of {} records to BQ. Insert success?: {}", records.getValidRecords().size(), !response.hasErrors());
            if (response.hasErrors()) {
                Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser.getErrorsFromBQResponse(records.getValidRecords(), response, bigQueryMetrics, instrumentation);
                errorInfoMap.forEach(odpfSinkResponse::addErrors);
                errorHandler.handle(response.getInsertErrors(), records.getValidRecords());
            }
        }
        return odpfSinkResponse;
    }
}
