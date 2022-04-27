package io.odpf.sink.connectors.config;

import io.odpf.sink.connectors.common.TupleString;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class BigQuerySinkConfigTest {

    @Test
    public void testMetadataTypes() {
        System.setProperty("SINK_CONNECTOR_SCHEMA_MESSAGE_CLASS", "io.odpf.sink.connectors.TestKeyBQ");
        System.setProperty("SINK_BIGQUERY_ENABLE_AUTO_SCHEMA_UPDATE", "false");
        System.setProperty("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "topic=string,partition=integer,offset=integer");
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, System.getProperties());
        List<TupleString> metadataColumnsTypes = config.getMetadataColumnsTypes();
        Assert.assertEquals(new ArrayList<TupleString>() {{
            add(new TupleString("topic", "string"));
            add(new TupleString("partition", "integer"));
            add(new TupleString("offset", "integer"));
        }}, metadataColumnsTypes);
    }
}
