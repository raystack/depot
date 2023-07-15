package org.raystack.depot.config;

import org.raystack.depot.config.converter.JsonToPropertiesConverter;
import org.raystack.depot.config.converter.RedisSinkDataTypeConverter;
import org.raystack.depot.config.converter.RedisSinkDeploymentTypeConverter;
import org.raystack.depot.config.converter.RedisSinkTtlTypeConverter;
import org.raystack.depot.redis.enums.RedisSinkDataType;
import org.raystack.depot.redis.enums.RedisSinkDeploymentType;
import org.raystack.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Config;

import java.util.Properties;

@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface RedisSinkConfig extends RaystackSinkConfig {
    @Key("SINK_REDIS_URLS")
    String getSinkRedisUrls();

    @Key("SINK_REDIS_KEY_TEMPLATE")
    String getSinkRedisKeyTemplate();

    @Key("SINK_REDIS_DATA_TYPE")
    @DefaultValue("HASHSET")
    @ConverterClass(RedisSinkDataTypeConverter.class)
    RedisSinkDataType getSinkRedisDataType();

    @Key("SINK_REDIS_TTL_TYPE")
    @DefaultValue("DISABLE")
    @ConverterClass(RedisSinkTtlTypeConverter.class)
    RedisSinkTtlType getSinkRedisTtlType();

    @Key("SINK_REDIS_TTL_VALUE")
    @DefaultValue("0")
    long getSinkRedisTtlValue();

    @Key("SINK_REDIS_DEPLOYMENT_TYPE")
    @DefaultValue("Standalone")
    @ConverterClass(RedisSinkDeploymentTypeConverter.class)
    RedisSinkDeploymentType getSinkRedisDeploymentType();

    @Key("SINK_REDIS_KEY_VALUE_DATA_FIELD_NAME")
    String getSinkRedisKeyValueDataFieldName();

    @Key("SINK_REDIS_LIST_DATA_FIELD_NAME")
    String getSinkRedisListDataFieldName();

    @Key("SINK_REDIS_HASHSET_FIELD_TO_COLUMN_MAPPING")
    @ConverterClass(JsonToPropertiesConverter.class)
    @DefaultValue("")
    Properties getSinkRedisHashsetFieldToColumnMapping();
}
