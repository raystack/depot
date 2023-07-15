package org.raystack.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import org.raystack.depot.bigquery.models.Record;

/**
 * Fetches BQ insertable row from the base record {@link Record}. The
 * implementations can differ if unique rows need to be inserted or not.
 */
public interface BigQueryRow {

    InsertAllRequest.RowToInsert of(Record record);
}
