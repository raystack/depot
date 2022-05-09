package io.odpf.depot.bigquery.handler;

import com.google.cloud.bigquery.BigQueryError;
import io.odpf.depot.bigquery.models.Record;

import java.util.List;
import java.util.Map;

public interface ErrorHandler {
    default void handle(Map<Long, List<BigQueryError>> errorInfoMap, List<Record> records) {
    }
}
