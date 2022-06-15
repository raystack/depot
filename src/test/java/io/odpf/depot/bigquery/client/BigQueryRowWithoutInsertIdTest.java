package io.odpf.depot.bigquery.client;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.depot.bigquery.models.Record;
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
