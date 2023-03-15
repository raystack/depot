package com.gotocompany.depot.message;

import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.SinkConfig;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;


public class MessageUtilsTest {
    private final Configuration configuration = Configuration.builder()
            .jsonProvider(new JsonOrgJsonProvider())
            .build();

    @Test
    public void shouldGetStringFieldFromJsonObject() {
        JSONObject object = new JSONObject("{\"test\" :\"test\"}");
        Assert.assertEquals("test", MessageUtils.getFieldFromJsonObject("test", object, configuration));
    }


    @Test
    public void shouldGetFieldFromNested() {
        JSONObject object = new JSONObject("{\"test\" :[{\"name\":\"John\",\"age\":50},{\"name\":\"Bob\",\"age\":60},{\"name\":\"Alice\",\"active\":true,\"height\":175}]}");
        Assert.assertEquals("Bob", MessageUtils.getFieldFromJsonObject("test[1].name", object, configuration));
        Assert.assertEquals(175, MessageUtils.getFieldFromJsonObject("test[2].height", object, configuration));
    }

    @Test
    public void shouldGetRepeatedField() {
        String jsonString = "{\n"
                + "  \"test\": [\n"
                + "    {\n"
                + "      \"name\": \"John\",\n"
                + "      \"age\": 50,\n"
                + "      \"alist\": [\n"
                + "        {\n"
                + "          \"name\": \"test\",\n"
                + "          \"value\": \"sometest\"\n"
                + "        },\n"
                + "        {\n"
                + "          \"name\": \"test2\",\n"
                + "          \"value\": \"sometest2\"\n"
                + "        }\n"
                + "      ]\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"John\",\n"
                + "      \"age\": 60\n"
                + "    },\n"
                + "    {\n"
                + "      \"name\": \"John\",\n"
                + "      \"active\": true,\n"
                + "      \"height\": 175\n"
                + "    }\n"
                + "  ]\n"
                + "}\n";

        JSONObject object = new JSONObject(jsonString);
        Assert.assertEquals(175, MessageUtils.getFieldFromJsonObject("test[2].height", object, configuration));
        Assert.assertEquals("[{\"name\":\"test\",\"value\":\"sometest\"},{\"name\":\"test2\",\"value\":\"sometest2\"}]", MessageUtils.getFieldFromJsonObject("test[0].alist", object, configuration).toString());
        Assert.assertEquals("sometest", MessageUtils.getFieldFromJsonObject("test[0].alist[0].value", object, configuration));
    }

    @Test
    public void shouldThrowExceptionIfInvalidPath() {
        JSONObject object = new JSONObject("{\"test\" :\"test\"}");
        IllegalArgumentException exception = Assert.assertThrows(IllegalArgumentException.class, () -> MessageUtils.getFieldFromJsonObject("testing", object, configuration));
        Assert.assertEquals("Invalid field config : testing", exception.getMessage());

        exception = Assert.assertThrows(IllegalArgumentException.class, () -> MessageUtils.getFieldFromJsonObject("test[0].testing", object, configuration));
        Assert.assertEquals("Invalid field config : test[0].testing", exception.getMessage());
    }

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
        SinkConfig config = ConfigFactory.create(SinkConfig.class, configMap);
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
        Message message = new Message(
                null,
                null,
                new Tuple<>("col1", "value1"),
                new Tuple<>("col2", "value2"),
                new Tuple<>("col3", 50000),
                new Tuple<>("col4", 1668158346000L));

        Map<String, String> configMap = new HashMap<>();
        configMap.put("SINK_ADD_METADATA_ENABLED", "true");
        configMap.put("SINK_METADATA_COLUMNS_TYPES", "col1=string,col2=string,col3=integer,col4=timestamp");
        SinkConfig config = ConfigFactory.create(SinkConfig.class, configMap);
        Function<Long, Object> timeStampConvertor = (Date::new);
        Map<String, Object> finalMetadata = MessageUtils.getMetaData(message, config, timeStampConvertor);
        Assert.assertEquals(4, finalMetadata.size());
        Assert.assertEquals("value1", finalMetadata.get("col1"));
        Assert.assertEquals("value2", finalMetadata.get("col2"));
        Assert.assertEquals(50000, finalMetadata.get("col3"));
        Assert.assertEquals(new Date(1668158346000L), finalMetadata.get("col4"));
    }

    @Test
    public void shouldThrowExceptionIfNotValid() {
        Message message = new Message("test", "test");
        IOException ioException = Assertions.assertThrows(IOException.class, () -> MessageUtils.validate(message, Integer.class));
        Assert.assertEquals("Expected class class java.lang.Integer, but found: LogKey class: class java.lang.String, LogMessage class: class java.lang.String", ioException.getMessage());
    }
}
