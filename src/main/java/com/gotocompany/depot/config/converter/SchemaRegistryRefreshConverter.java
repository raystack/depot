package com.gotocompany.depot.config.converter;

import com.gotocompany.stencil.cache.SchemaRefreshStrategy;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;

public class SchemaRegistryRefreshConverter implements Converter<SchemaRefreshStrategy> {

    @Override
    public SchemaRefreshStrategy convert(Method method, String input) {
        if ("VERSION_BASED_REFRESH".equalsIgnoreCase(input)) {
            return SchemaRefreshStrategy.versionBasedRefresh();
        }
        return SchemaRefreshStrategy.longPollingStrategy();
    }
}
