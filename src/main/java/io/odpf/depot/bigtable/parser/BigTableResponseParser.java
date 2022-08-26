package io.odpf.depot.bigtable.parser;

import io.odpf.depot.bigtable.model.BigTableRecord;
import io.odpf.depot.bigtable.response.BigTableResponse;
import io.odpf.depot.error.ErrorInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigTableResponseParser {
    public static Map<Long, ErrorInfo> parseAndFillOdpfSinkResponse(List<BigTableRecord> validRecords, BigTableResponse bigTableResponse) {
        return new HashMap<>();
    }
}
