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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BigTableSink implements OdpfSink {
    private final BigTableClient bigTableClient;
    private final BigTableRecordParser bigTableRecordParser;

    public BigTableSink(BigTableClient bigTableClient, BigTableRecordParser bigTableRecordParser) {
        this.bigTableClient = bigTableClient;
        this.bigTableRecordParser = bigTableRecordParser;
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
            if (bigTableResponse != null && bigTableResponse.hasErrors()) {
                Map<Long, ErrorInfo> errorInfoMap = BigTableResponseParser.parseAndFillOdpfSinkResponse(validRecords, bigTableResponse);
                errorInfoMap.forEach(odpfSinkResponse::addErrors);
            }
        }

        return odpfSinkResponse;
    }

    @Override
    public void close() throws IOException {
    }
}
