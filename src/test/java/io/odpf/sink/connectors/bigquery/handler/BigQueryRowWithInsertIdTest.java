package io.odpf.sink.connectors.bigquery.handler;

import com.google.cloud.bigquery.InsertAllRequest;
import io.odpf.sink.connectors.bigquery.handler.BigQueryRowWithInsertId;
import io.odpf.sink.connectors.bigquery.models.Record;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;


public class BigQueryRowWithInsertIdTest {

    @Test
    public void shouldCreateRowWithInsertID() {
        Record record = new Record(new HashMap<>(), new HashMap<>(), 0, null);

        BigQueryRowWithInsertId withInsertId = new BigQueryRowWithInsertId(metadata -> "default_1_1");
        InsertAllRequest.RowToInsert rowToInsert = withInsertId.of(record);
        String id = rowToInsert.getId();

        assertEquals("default_1_1", id);
    }
}
