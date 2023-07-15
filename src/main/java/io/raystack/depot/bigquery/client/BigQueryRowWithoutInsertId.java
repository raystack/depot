package org.raystack.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import org.raystack.depot.bigquery.models.Record;

public class BigQueryRowWithoutInsertId implements BigQueryRow {

    @Override
    public InsertAllRequest.RowToInsert of(Record record) {
        return InsertAllRequest.RowToInsert.of(record.getColumns());
    }
}
