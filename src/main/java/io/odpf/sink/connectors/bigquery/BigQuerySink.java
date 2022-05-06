package io.odpf.sink.connectors.bigquery;

import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import io.odpf.sink.connectors.bigquery.handler.MessageRecordConverterCache;
import io.odpf.sink.connectors.bigquery.handler.ErrorHandler;
import io.odpf.sink.connectors.error.ErrorInfo;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.OdpfSink;
import io.odpf.sink.connectors.OdpfSinkResponse;
import io.odpf.sink.connectors.bigquery.handler.BigQueryClient;
import io.odpf.sink.connectors.bigquery.handler.BigQueryResponseParser;
import io.odpf.sink.connectors.bigquery.handler.BigQueryRow;
import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.bigquery.models.Records;
import io.odpf.sink.connectors.metrics.BigQueryMetrics;
import io.odpf.sink.connectors.metrics.Instrumentation;

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
                Map<Long, ErrorInfo> errorInfoMap = BigQueryResponseParser.parseAndFillOdpfSinkResponse(records.getValidRecords(), response, bigQueryMetrics, instrumentation);
                errorInfoMap.forEach(odpfSinkResponse::addErrors);
                errorHandler.handle(response.getInsertErrors(), records.getValidRecords());
            }
        }
        return odpfSinkResponse;
    }
}
