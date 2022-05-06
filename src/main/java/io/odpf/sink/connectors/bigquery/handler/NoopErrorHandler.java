package io.odpf.sink.connectors.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import io.odpf.sink.connectors.bigquery.models.Record;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

@Slf4j
public class NoopErrorHandler implements ErrorHandler {
    @Override
    public void handle(Map<Long, List<BigQueryError>> errorInfoMap, List<Record> records) {

    }
}
