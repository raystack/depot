package io.odpf.depot.message;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.spi.json.JsonOrgJsonProvider;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

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
    public void shouldNotThrowExceptionIfValid() throws IOException {
        OdpfMessage message = new OdpfMessage("test", "test");
        MessageUtils.validate(message, String.class);

    }
    @Test
    public void shouldThrowExceptionIfNotValid() {
        OdpfMessage message = new OdpfMessage("test", "test");
        IOException ioException = Assertions.assertThrows(IOException.class, () -> MessageUtils.validate(message, Integer.class));
        Assert.assertEquals("Expected class class java.lang.Integer, but found: LogKey class: class java.lang.String, LogMessage class: class java.lang.String", ioException.getMessage());
    }
}
