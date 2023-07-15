package org.raystack.depot.bigtable.model;

import org.raystack.depot.exception.ConfigurationException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

public class BigTableSchema {

    private final JSONObject columnFamilyMapping;

    public BigTableSchema(String columnMapping) {
        if (columnMapping == null || columnMapping.isEmpty()) {
            throw new ConfigurationException("Column Mapping should not be empty or null");
        }
        this.columnFamilyMapping = new JSONObject(columnMapping);
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

    /**
     * Returns missing column families.
     *
     * @param existingColumnFamilies existing column families in a table.
     * @return set of missing column families
     */
    public Set<String> getMissingColumnFamilies(Set<String> existingColumnFamilies) {
        Set<String> tempSet = new HashSet<>(getColumnFamilies());
        tempSet.removeAll(existingColumnFamilies);
        return tempSet;
    }
}
