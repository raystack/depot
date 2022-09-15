package io.odpf.depot.config;

import io.odpf.depot.config.converter.SinkConnectorSchemaDataTypeConverter;
import io.odpf.depot.config.enums.SinkConnectorSchemaDataType;
import io.odpf.depot.config.converter.SinkConnectorSchemaMessageModeConverter;
import io.odpf.depot.config.converter.SchemaRegistryHeadersConverter;
import io.odpf.depot.config.converter.SchemaRegistryRefreshConverter;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
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
    @DefaultValue("VERSION_BASED_REFRESH")
    SchemaRefreshStrategy getSchemaRegistryStencilRefreshStrategy();

    @Key("SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS")
    @TokenizerClass(SchemaRegistryHeadersConverter.class)
    @ConverterClass(SchemaRegistryHeadersConverter.class)
    @DefaultValue("")
    List<Header> getSchemaRegistryStencilFetchHeaders();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_AUTO_REFRESH")
    @DefaultValue("true")
    Boolean getSchemaRegistryStencilCacheAutoRefresh();

    @Key("SCHEMA_REGISTRY_STENCIL_CACHE_TTL_MS")
    @DefaultValue("900000")
    Long getSchemaRegistryStencilCacheTtlMs();

    @Key("SCHEMA_REGISTRY_STENCIL_URLS")
    String getSchemaRegistryStencilUrls();

    @Key("SINK_METRICS_APPLICATION_PREFIX")
    @DefaultValue("application_")
    String getMetricsApplicationPrefix();

    @Key("SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS")
    @DefaultValue("")
    String getSinkConnectorSchemaProtoMessageClass();

    @Key("SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS")
    @DefaultValue("")
    String getSinkConnectorSchemaProtoKeyClass();

    @Key("SINK_CONNECTOR_SCHEMA_JSON_PARSER_STRING_MODE_ENABLED")
    @DefaultValue("true")
    boolean getSinkConnectorSchemaJsonParserStringModeEnabled();

    @Key("SINK_CONNECTOR_SCHEMA_DATA_TYPE")
    @ConverterClass(SinkConnectorSchemaDataTypeConverter.class)
    @DefaultValue("PROTOBUF")
    SinkConnectorSchemaDataType getSinkConnectorSchemaDataType();

    @Key("SINK_CONNECTOR_SCHEMA_MESSAGE_MODE")
    @ConverterClass(SinkConnectorSchemaMessageModeConverter.class)
    @DefaultValue("LOG_MESSAGE")
    SinkConnectorSchemaMessageMode getSinkConnectorSchemaMessageMode();

    @Key("SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE")
    @DefaultValue("false")
    boolean getSinkConnectorSchemaProtoAllowUnknownFieldsEnable();
}
