package com.gotocompany.depot.utils;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.stencil.SchemaUpdateListener;
import com.gotocompany.stencil.config.StencilConfig;
import com.timgroup.statsd.StatsDClient;

public class StencilUtils {
    public static StencilConfig getStencilConfig(
            SinkConfig sinkConfig,
            StatsDClient statsDClient,
            SchemaUpdateListener schemaUpdateListener) {
        return StencilConfig.builder()
                .cacheAutoRefresh(sinkConfig.getSchemaRegistryStencilCacheAutoRefresh())
                .cacheTtlMs(sinkConfig.getSchemaRegistryStencilCacheTtlMs())
                .statsDClient(statsDClient)
                .fetchHeaders(sinkConfig.getSchemaRegistryStencilFetchHeaders())
                .fetchBackoffMinMs(sinkConfig.getSchemaRegistryStencilFetchBackoffMinMs())
                .fetchRetries(sinkConfig.getSchemaRegistryStencilFetchRetries())
                .fetchTimeoutMs(sinkConfig.getSchemaRegistryStencilFetchTimeoutMs())
                .refreshStrategy(sinkConfig.getSchemaRegistryStencilRefreshStrategy())
                .updateListener(schemaUpdateListener)
                .build();
    }

    public static StencilConfig getStencilConfig(SinkConfig config, StatsDClient statsDClient) {
        return getStencilConfig(config, statsDClient, null);
    }
}
