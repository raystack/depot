package io.odpf.depot.bigtable.model;

import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import org.aeonbits.owner.ConfigFactory;
import org.json.JSONException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class BigtableSchemaTest {
    private BigtableSchema bigtableSchema;

    @Before
    public void setUp() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{\n"
                + "\"family_name1\" : {\n"
                + "\"qualifier_name1\" : \"data.is_complete\",\n"
                + "\"qualifier_name2\" : \"data.content\"\n"
                + "},\n"
                + "\"family_name2\" : {\n"
                + "\"qualifier_name3\" : \"base_content3\",\n"
                + "\"qualifier_name4\" : \"base_content4\"\n"
                + "}\n"
                + "}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigtableSchema(sinkConfig.getColumnFamilyMapping());
    }

    @Test
    public void shouldGeSetOfColumnFamilies() {
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        assertEquals(2, columnFamilies.size());
        assertTrue(columnFamilies.contains("family_name1"));
        assertTrue(columnFamilies.contains("family_name2"));
    }

    @Test
    public void shouldGetFieldNameForGivenColumnFamilyAndQualifier() {
        assertEquals("data.is_complete", bigtableSchema.getField("family_name1", "qualifier_name1"));
        assertEquals("data.content", bigtableSchema.getField("family_name1", "qualifier_name2"));

        assertEquals("base_content3", bigtableSchema.getField("family_name2", "qualifier_name3"));
        assertEquals("base_content4", bigtableSchema.getField("family_name2", "qualifier_name4"));
    }

    @Test
    public void shouldGetColumnsForGivenColumnFamily() {
        assertEquals(2, bigtableSchema.getColumns("family_name1").size());
        assertTrue(bigtableSchema.getColumns("family_name1").contains("qualifier_name1"));
        assertTrue(bigtableSchema.getColumns("family_name1").contains("qualifier_name2"));

        assertEquals(2, bigtableSchema.getColumns("family_name2").size());
        assertTrue(bigtableSchema.getColumns("family_name2").contains("qualifier_name3"));
        assertTrue(bigtableSchema.getColumns("family_name2").contains("qualifier_name4"));
    }

    @Test
    public void shouldThrowConfigurationExceptionWhenColumnMappingIsEmpty() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        ConfigurationException configurationException = assertThrows(ConfigurationException.class, () -> new BigtableSchema(sinkConfig.getColumnFamilyMapping()));
        Assert.assertEquals("Column Mapping should not be empty or null", configurationException.getMessage());
    }

    @Test
    public void shouldReturnEmptySetIfNoColumnFamilies() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigtableSchema(sinkConfig.getColumnFamilyMapping());
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        Assert.assertEquals(0, columnFamilies.size());
    }

    @Test
    public void shouldReturnEmptySetIfNoColumnsPresent() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{\n"
                + "\"family_name1\" : {\n"
                + "\"qualifier_name1\" : \"data.is_complete\",\n"
                + "\"qualifier_name2\" : \"data.content\"\n"
                + "},\n"
                + "\"family_name2\" : {}\n"
                + "}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigtableSchema(sinkConfig.getColumnFamilyMapping());
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        Assert.assertEquals(2, columnFamilies.size());
        Set<String> columns = bigtableSchema.getColumns("family_name2");
        Assert.assertEquals(0, columns.size());
    }

    @Test
    public void shouldThrowJsonException() {
        System.setProperty("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING", "{\n"
                + "\"family_name1\" : {\n"
                + "\"qualifier_name1\" : \"data.is_complete\",\n"
                + "\"qualifier_name2\" : \"data.content\"\n"
                + "},\n"
                + "\"family_name2\" : {}\n"
                + "}");
        BigTableSinkConfig sinkConfig = ConfigFactory.create(BigTableSinkConfig.class, System.getProperties());
        bigtableSchema = new BigtableSchema(sinkConfig.getColumnFamilyMapping());
        Set<String> columnFamilies = bigtableSchema.getColumnFamilies();
        Assert.assertEquals(2, columnFamilies.size());
        JSONException jsonException = assertThrows(JSONException.class, () -> bigtableSchema.getColumns("family_name3"));
        Assert.assertEquals("JSONObject[\"family_name3\"] not found.", jsonException.getMessage());

        jsonException = assertThrows(JSONException.class, () -> bigtableSchema.getField("family_name1", "qualifier_name3"));
        Assert.assertEquals("JSONObject[\"qualifier_name3\"] not found.", jsonException.getMessage());
    }

    @Test
    public void shouldReturnEmptySetOfMissingColumnFamilies() {
        Set<String> missingColumnFamilies = bigtableSchema.getMissingColumnFamilies(new HashSet<String>() {{
            add("family_name1");
            add("family_name2");
        }});
        Assert.assertEquals(0, missingColumnFamilies.size());
    }

    @Test
    public void shouldReturnMissingColumnFamilies() {
        Set<String> missingColumnFamilies = bigtableSchema.getMissingColumnFamilies(new HashSet<String>() {{
            add("family_name3");
            add("family_name2");
            add("family_name4");
        }});
        Assert.assertEquals(new HashSet<String>() {{
            add("family_name1");
        }}, missingColumnFamilies);
    }
}
