package io.odpf.depot.bigtable;

import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.bigtable.client.BigTableClient;
import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.parser.BigTableRecordParser;
import io.odpf.depot.bigtable.parser.BigTableResponseParser;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.message.OdpfMessage;
import io.odpf.depot.metrics.BigTableMetrics;
import io.odpf.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigTableSink implements OdpfSink {
    private final BigTableClient bigTableClient;
    private final BigTableRecordParser bigTableRecordParser;
    private final BigTableMetrics bigtableMetrics;
    private final Instrumentation instrumentation;

    public BigTableSink(BigTableClient bigTableClient, BigTableRecordParser bigTableRecordParser, BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        this.bigTableClient = bigTableClient;
        this.bigTableRecordParser = bigTableRecordParser;
        this.bigtableMetrics = bigtableMetrics;
        this.instrumentation = instrumentation;
    }

    @Override
    public OdpfSinkResponse pushToSink(List<OdpfMessage> messages) {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        Map<Boolean, List<BigTableRecord>> splitterRecords = records.stream().collect(Collectors.partitioningBy(BigTableRecord::isValid));
        List<BigTableRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<BigTableRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        OdpfSinkResponse odpfSinkResponse = new OdpfSinkResponse();
        invalidRecords.forEach(invalidRecord -> odpfSinkResponse.addErrors(invalidRecord.getIndex(), invalidRecord.getErrorInfo()));

        if (validRecords.size() > 0) {
            BigTableResponse bigTableResponse = bigTableClient.send(validRecords);
            instrumentation.logInfo("Processed a batch of {} records to BigTable", validRecords.size());
            if (bigTableResponse != null && bigTableResponse.hasErrors()) {
                instrumentation.logInfo("Found {} Error records in response", bigTableResponse.getErrorCount());
                Map<Long, ErrorInfo> errorInfoMap = BigTableResponseParser.getErrorsFromSinkResponse(validRecords, bigTableResponse, bigtableMetrics, instrumentation);
                errorInfoMap.forEach(odpfSinkResponse::addErrors);
            }
        }

        return odpfSinkResponse;
    }

    @Override
    public void close() throws IOException {
    }
}
