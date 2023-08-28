package com.gotocompany.depot.config;

import com.gotocompany.depot.config.converter.JsonToPropertiesConverter;
import com.gotocompany.depot.config.converter.RedisSinkDataTypeConverter;
import com.gotocompany.depot.config.converter.RedisSinkDeploymentTypeConverter;
import com.gotocompany.depot.config.converter.RedisSinkTtlTypeConverter;
import com.gotocompany.depot.redis.enums.RedisSinkDataType;
import com.gotocompany.depot.redis.enums.RedisSinkDeploymentType;
import com.gotocompany.depot.redis.enums.RedisSinkTtlType;
import org.aeonbits.owner.Config;

import java.util.Properties;


@Config.DisableFeature(Config.DisableableFeature.PARAMETER_FORMATTING)
public interface RedisSinkConfig extends SinkConfig {
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

    @Key("SINK_REDIS_DEFAULT_FIELD_VALUE_ENABLE")
    @DefaultValue("true")
    boolean getSinkDefaultFieldValueEnable();
}
