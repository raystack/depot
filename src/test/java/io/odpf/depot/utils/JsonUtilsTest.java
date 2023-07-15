package org.raystack.depot.utils;

import org.raystack.depot.config.OdpfSinkConfig;
import org.junit.Assert;
import org.junit.Test;
import org.json.JSONObject;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JsonUtilsTest {
    @Mock
    private OdpfSinkConfig raystackSinkConfig;

    void setSinkConfigs(boolean stringModeEnabled) {
        when(raystackSinkConfig.getSinkConnectorSchemaJsonParserStringModeEnabled()).thenReturn(stringModeEnabled);
    }

    @Test
    public void shouldParseSimpleJsonWhenStringModeEnabled() {
        setSinkConfigs(true);
        JSONObject expectedJson = new JSONObject();
        expectedJson.put("name", "foo");
        expectedJson.put("num", "0371480");
        expectedJson.put("balance", "100");
        expectedJson.put("is_vip", "YES");
        byte[] payload = expectedJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(raystackSinkConfig, payload);
        Assert.assertTrue(parsedJson.similar(expectedJson));
    }

    @Test
    public void shouldCastAllTypeToStringWhenStringModeEnabled() {
        setSinkConfigs(true);
        JSONObject originalJson = new JSONObject();
        originalJson.put("name", "foo");
        originalJson.put("num", new Integer(100));
        originalJson.put("balance", new Double(1000.21));
        originalJson.put("is_vip", Boolean.TRUE);
        byte[] payload = originalJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(raystackSinkConfig, payload);
        JSONObject stringJson = new JSONObject();
        stringJson.put("name", "foo");
        stringJson.put("num", "100");
        stringJson.put("balance", "1000.21");
        stringJson.put("is_vip", "true");
        Assert.assertTrue(parsedJson.similar(stringJson));
    }

    @Test
    public void shouldThrowExceptionForNestedJsonWhenStringModeEnabled() {
        setSinkConfigs(true);
        JSONObject nestedJsonField = new JSONObject();
        nestedJsonField.put("name", "foo");
        nestedJsonField.put("num", "0371480");
        nestedJsonField.put("balance", "100");
        nestedJsonField.put("is_vip", "YES");
        JSONObject nestedJson = new JSONObject();
        nestedJson.put("ID", 1);
        nestedJson.put("nestedField", nestedJsonField);
        byte[] payload = nestedJson.toString().getBytes();
        UnsupportedOperationException exception = assertThrows(UnsupportedOperationException.class,
                () -> JsonUtils.getJsonObject(raystackSinkConfig, payload));
        assertEquals("nested json structure not supported yet", exception.getMessage());
    }

    @Test
    public void shouldParseSimpleJsonWhenStringModeDisabled() {
        setSinkConfigs(false);
        JSONObject expectedJson = new JSONObject();
        expectedJson.put("name", "foo");
        expectedJson.put("num", new Integer(100));
        expectedJson.put("balance", new Double(1000.21));
        expectedJson.put("is_vip", Boolean.TRUE);
        byte[] payload = expectedJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(raystackSinkConfig, payload);
        Assert.assertTrue(parsedJson.similar(expectedJson));
    }

    @Test
    public void shouldParseNestedJsonWhenStringModeDisabled() {
        setSinkConfigs(false);
        JSONObject nestedJsonField = new JSONObject();
        nestedJsonField.put("name", "foo");
        nestedJsonField.put("num", new Integer(100));
        nestedJsonField.put("balance", new Double(1000.21));
        nestedJsonField.put("is_vip", Boolean.TRUE);
        JSONObject nestedJson = new JSONObject();
        nestedJson.put("ID", 1);
        nestedJson.put("nestedField", nestedJsonField);
        byte[] payload = nestedJson.toString().getBytes();
        JSONObject parsedJson = JsonUtils.getJsonObject(raystackSinkConfig, payload);
        Assert.assertTrue(parsedJson.similar(nestedJson));
    }
}
