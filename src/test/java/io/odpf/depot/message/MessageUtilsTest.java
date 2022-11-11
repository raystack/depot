package io.odpf.depot.message;

import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.OdpfSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class MessageUtilsTest {

    @Test
    public void shouldCheckAndSetTimeStampColumns() {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("col1", "value1");
        metadata.put("col2", "value2");
        metadata.put("col3", 50000);
        metadata.put("col4", 1668158346000L);

        Map<String, String> configMap = new HashMap<>();
        configMap.put("SINK_ADD_METADATA_ENABLED", "true");
        configMap.put("SINK_METADATA_COLUMNS_TYPES", "col1=string,col2=string,col3=integer,col4=timestamp");
        OdpfSinkConfig config = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        Function<Long, Object> timeStampConvertor = (Date::new);
        Map<String, Object> finalMetadata = MessageUtils.checkAndSetTimeStampColumns(metadata, config.getMetadataColumnsTypes(), timeStampConvertor);

        Assert.assertEquals(4, finalMetadata.size());
        Assert.assertEquals("value1", finalMetadata.get("col1"));
        Assert.assertEquals("value2", finalMetadata.get("col2"));
        Assert.assertEquals(50000, finalMetadata.get("col3"));
        Assert.assertEquals(new Date(1668158346000L), finalMetadata.get("col4"));
    }

    @Test
    public void shouldReturnMetadata() {
        OdpfMessage odpfMessage = new OdpfMessage(
                null,
                null,
                new Tuple<>("col1", "value1"),
                new Tuple<>("col2", "value2"),
                new Tuple<>("col3", 50000),
                new Tuple<>("col4", 1668158346000L));

        Map<String, String> configMap = new HashMap<>();
        configMap.put("SINK_ADD_METADATA_ENABLED", "true");
        configMap.put("SINK_METADATA_COLUMNS_TYPES", "col1=string,col2=string,col3=integer,col4=timestamp");
        OdpfSinkConfig config = ConfigFactory.create(OdpfSinkConfig.class, configMap);
        Function<Long, Object> timeStampConvertor = (Date::new);
        Map<String, Object> finalMetadata = MessageUtils.getMetaData(odpfMessage, config, timeStampConvertor);
        Assert.assertEquals(4, finalMetadata.size());
        Assert.assertEquals("value1", finalMetadata.get("col1"));
        Assert.assertEquals("value2", finalMetadata.get("col2"));
        Assert.assertEquals(50000, finalMetadata.get("col3"));
        Assert.assertEquals(new Date(1668158346000L), finalMetadata.get("col4"));
    }
}
