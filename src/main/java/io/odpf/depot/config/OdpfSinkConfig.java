package io.odpf.depot.config;

import io.odpf.depot.config.converter.InputSchemaDataTypeConverter;
import io.odpf.depot.config.enums.InputSchemaDataType;
import io.odpf.depot.config.converter.InputSchemaMessageModeConverter;
import io.odpf.depot.config.converter.SchemaRegistryHeadersConverter;
import io.odpf.depot.config.converter.SchemaRegistryRefreshConverter;
import io.odpf.depot.message.InputSchemaMessageMode;
import io.odpf.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.Config;
import org.apache.http.Header;

import java.util.List;

public interface OdpfSinkConfig extends Config {

    @Key("SCHEMA_REGISTRY_STENCIL_ENABLE")
    @DefaultValue("false")
    Boolean isSchemaRegistryStencilEnable();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS")
    @DefaultValue("10000")
    Integer getSchemaRegistryStencilFetchTimeoutMs();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_RETRIES")
    @DefaultValue("4")
    Integer getSchemaRegistryStencilFetchRetries();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_BACKOFF_MIN_MS")
    @DefaultValue("60000")
    Long getSchemaRegistryStencilFetchBackoffMinMs();

    @Key("SCHEMA_REGISTRY_STENCIL_REFRESH_STRATEGY")
    @ConverterClass(SchemaRegistryRefreshConverter.class)
    SchemaRefreshStrategy getSchemaRegistryStencilRefreshStrategy();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSchemaRegistryFetchHeaders();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("false")
    Boolean getSchemaRegistryStencilCacheAutoRefresh();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSchemaRegistryStencilCacheTtlMs();

    @Key("SCHEMA_REGISTRY_STENCIL_URLS")
    String getSchemaRegistryStencilUrls();

    @Key("SINK_METRICS_APPLICATION_PREFIX")
    @DefaultValue("application_")
    String getMetricsApplicationPrefix();

    @Key("SINK_CONNECTOR_SCHEMA_MESSAGE_CLASS")
    @DefaultValue("")
    String getSinkConnectorSchemaMessageClass();

    @Key("SINK_CONNECTOR_SCHEMA_KEY_CLASS")
    @DefaultValue("")
    String getSinkConnectorSchemaKeyClass();


    @Key("SINK_CONNECTOR_SCHEMA_DATA_TYPE")
    @ConverterClass(InputSchemaDataTypeConverter.class)
    @DefaultValue("PROTOBUF")
    InputSchemaDataType getSinkConnectorSchemaDataTye();

    @Key("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE")
    @ConverterClass(InputSchemaMessageModeConverter.class)
    @DefaultValue("LOG_MESSAGE")
    InputSchemaMessageMode getSinkConnectorSchemaMessageMode();

    @Key("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE")
    @DefaultValue("false")
    boolean getSinkConnectorSchemaProtoAllowUnknownFieldsEnable();
}
