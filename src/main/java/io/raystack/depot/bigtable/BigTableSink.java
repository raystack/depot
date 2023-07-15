package org.raystack.depot.bigtable;

import org.raystack.depot.RaystackSink;
import org.raystack.depot.RaystackSinkResponse;
import org.raystack.depot.bigtable.client.BigTableClient;
import org.raystack.depot.bigtable.model.BigTableRecord;
import org.raystack.depot.bigtable.parser.BigTableRecordParser;
import org.raystack.depot.bigtable.parser.BigTableResponseParser;
import org.raystack.depot.bigtable.response.BigTableResponse;
import org.raystack.depot.error.ErrorInfo;
import org.raystack.depot.message.RaystackMessage;
import org.raystack.depot.metrics.BigTableMetrics;
import org.raystack.depot.metrics.Instrumentation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigTableSink implements RaystackSink {
    private final BigTableClient bigTableClient;
    private final BigTableRecordParser bigTableRecordParser;
    private final BigTableMetrics bigtableMetrics;
    private final Instrumentation instrumentation;

    public BigTableSink(BigTableClient bigTableClient, BigTableRecordParser bigTableRecordParser,
            BigTableMetrics bigtableMetrics, Instrumentation instrumentation) {
        this.bigTableClient = bigTableClient;
        this.bigTableRecordParser = bigTableRecordParser;
        this.bigtableMetrics = bigtableMetrics;
        this.instrumentation = instrumentation;
    }

    @Override
    public RaystackSinkResponse pushToSink(List<RaystackMessage> messages) {
        List<BigTableRecord> records = bigTableRecordParser.convert(messages);
        Map<Boolean, List<BigTableRecord>> splitterRecords = records.stream()
                .collect(Collectors.partitioningBy(BigTableRecord::isValid));
        List<BigTableRecord> invalidRecords = splitterRecords.get(Boolean.FALSE);
        List<BigTableRecord> validRecords = splitterRecords.get(Boolean.TRUE);

        RaystackSinkResponse raystackSinkResponse = new RaystackSinkResponse();
        invalidRecords.forEach(
                invalidRecord -> raystackSinkResponse.addErrors(invalidRecord.getIndex(),
                        invalidRecord.getErrorInfo()));

        if (validRecords.size() > 0) {
            BigTableResponse bigTableResponse = bigTableClient.send(validRecords);
            if (bigTableResponse != null && bigTableResponse.hasErrors()) {
                instrumentation.logInfo("Found {} Error records in response", bigTableResponse.getErrorCount());
                Map<Long, ErrorInfo> errorInfoMap = BigTableResponseParser.getErrorsFromSinkResponse(validRecords,
                        bigTableResponse, bigtableMetrics, instrumentation);
                errorInfoMap.forEach(raystackSinkResponse::addErrors);
            }
        }

        return raystackSinkResponse;
    }

    @Override
    public void close() throws IOException {
    }
}
