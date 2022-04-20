package io.odpf.sink.connectors.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.sink.connectors.bigquery.models.Record;

import java.util.Map;
import java.util.function.Function;

public class BigQueryRowWithInsertId implements BigQueryRow {
    private final Function<Map<String, Object>, String> rowIDCreator;

    public BigQueryRowWithInsertId(Function<Map<String, Object>, String> rowIDCreator) {
        this.rowIDCreator = rowIDCreator;
    }

    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(rowIDCreator.apply(record.getMetadata()), record.getColumns());
    }
}
