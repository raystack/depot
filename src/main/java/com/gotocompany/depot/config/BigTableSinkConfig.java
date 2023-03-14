package com.gotocompany.depot.config;

import org.aeonbits.owner.Config;

@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface BigTableSinkConfig extends SinkConfig {
    @Key("SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID")
    String getGCloudProjectID();

    @Key("SINK_BIGTABLE_INSTANCE_ID")
    String getInstanceId();

    @Key("SINK_BIGTABLE_TABLE_ID")
    String getTableId();

    @Key("SINK_BIGTABLE_CREDENTIAL_PATH")
    String getCredentialPath();

    @Key("SINK_BIGTABLE_ROW_KEY_TEMPLATE")
    String getRowKeyTemplate();

    @Key("SINK_BIGTABLE_COLUMN_FAMILY_MAPPING")
    String getColumnFamilyMapping();
}
