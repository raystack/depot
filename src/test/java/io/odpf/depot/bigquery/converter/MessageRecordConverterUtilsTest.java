package io.odpf.depot.bigquery.converter;

import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.message.OdpfMessage;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class MessageRecordConverterUtilsTest {

    @Test
    public void shouldAddMetaData() {
        Map<String, Object> columns = new HashMap<String, Object>() {{
            put("test", 123);
        }};
        OdpfMessage message = Mockito.mock(OdpfMessage.class);
        Mockito.when(message.getMetadata(Mockito.any())).thenReturn(new HashMap<String, Object>() {{
            put("test2", "value2");
            put("something", 99L);
            put("nvm", "nvm");
        }});
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, new HashMap<String, String>() {{
            put("SINK_BIGQUERY_ADD_METADATA_ENABLED", "true");
            put("SINK_BIGQUERY_METADATA_COLUMNS_TYPES", "test2=string,something=long,nvm=string");
        }});
        MessageRecordConverterUtils.addMetadata(columns, message, config);
        Assert.assertEquals(new HashMap<String, Object>() {{
            put("test", 123);
            put("test2", "value2");
            put("something", 99L);
            put("nvm", "nvm");
        }}, columns);
    }

    @Test
    public void shouldAddTimeStampForJson() {
        Map<String, Object> columns = new HashMap<String, Object>() {{
            put("test", 123);
        }};
        BigQuerySinkConfig config = ConfigFactory.create(BigQuerySinkConfig.class, new HashMap<String, String>() {{
            put("SINK_CONNECTOR_SCHEMA_DATA_TYPE", "json");
            put("SINK_BIGQUERY_ADD_EVENT_TIMESTAMP_ENABLE", "true");
        }});
        MessageRecordConverterUtils.addTimeStampColumnForJson(columns, config);
        Assert.assertEquals(2, columns.size());
        Assert.assertNotNull(columns.get("event_timestamp"));
    }
}
