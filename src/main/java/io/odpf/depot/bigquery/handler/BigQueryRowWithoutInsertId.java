package io.odpf.depot.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.depot.bigquery.models.Record;

public class BigQueryRowWithoutInsertId implements BigQueryRow {

    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(record.getColumns());
    }
}
