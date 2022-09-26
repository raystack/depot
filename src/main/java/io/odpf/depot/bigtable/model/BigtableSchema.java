package io.odpf.depot.bigtable.model;

import io.odpf.depot.config.BigTableSinkConfig;
import org.json.JSONObject;

import java.util.Set;

public class BigtableSchema {

    private final JSONObject columnFamilyMapping;

    public BigtableSchema(BigTableSinkConfig sinkConfig) {
        this.columnFamilyMapping = new JSONObject(sinkConfig.getColumnFamilyMapping());
    }

    public String getField(String columnFamily, String columnName) {
        JSONObject columns = columnFamilyMapping.getJSONObject(columnFamily);
        return columns.getString(columnName);
    }

    public Set<String> getColumnFamilies() {
        return columnFamilyMapping.keySet();
    }

    public Set<String> getColumns(String family) {
        return columnFamilyMapping.getJSONObject(family).keySet();
    }

}
