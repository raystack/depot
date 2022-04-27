package io.odpf.sink.connectors.bigquery.handler;

import io.odpf.sink.connectors.bigquery.models.Record;
import io.odpf.sink.connectors.error.ErrorInfo;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class NoopErrorHandler implements ErrorHandler {
    @Override
    public void handle(Map<Long, ErrorInfo> errorInfoMap, List<Record> records) {
    }
}
