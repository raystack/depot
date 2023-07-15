package org.raystack.depot.utils;

import com.timgroup.statsd.StatsDClient;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.stencil.SchemaUpdateListener;
import org.raystack.stencil.config.StencilConfig;

public class StencilUtils {
    public static StencilConfig getStencilConfig(
            RaystackSinkConfig sinkConfig,
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

    public static StencilConfig getStencilConfig(RaystackSinkConfig config, StatsDClient statsDClient) {
        return getStencilConfig(config, statsDClient, null);
    }
}
