package io.odpf.depot.bigtable.model;

import io.odpf.depot.config.BigTableSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Before;
import org.junit.Test;

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
        bigtableSchema = new BigtableSchema(sinkConfig);
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
}
