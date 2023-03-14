package com.gotocompany.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import com.gotocompany.depot.bigquery.models.Record;

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
