package org.raystack.depot.bigquery.storage.json;

import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import org.raystack.depot.bigquery.storage.BigQueryStream;

public class BigQueryJsonStream implements BigQueryStream {

    public JsonStreamWriter getStreamWriter() {
        return null;
    }
}
