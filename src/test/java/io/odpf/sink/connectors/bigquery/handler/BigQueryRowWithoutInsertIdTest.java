package io.odpf.sink.connectors.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.sink.connectors.bigquery.handler.BigQueryRowWithoutInsertId;
import io.odpf.sink.connectors.bigquery.models.Record;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertNull;

public class BigQueryRowWithoutInsertIdTest {

    @Test
    public void shouldCreateRowWithoutInsertID() {
        Record record = new Record(new HashMap<>(), new HashMap<>(), 0, null);

        BigQueryRowWithoutInsertId withoutInsertId = new BigQueryRowWithoutInsertId();
        InsertAllRequest.RowToInsert rowToInsert = withoutInsertId.of(record);
        String id = rowToInsert.getId();

        assertNull(id);
    }
}
