package io.odpf.sink.connectors.bigquery.handler;

import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.error.ErrorInfo;

import java.util.List;
import java.util.Map;

public interface ErrorHandler {
    void handle(Map<Long, ErrorInfo> errorInfoMap, List<Record> records);
}
