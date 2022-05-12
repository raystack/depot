package io.odpf.depot.utils;

import com.timgroup.statsd.StatsDClient;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.stencil.SchemaUpdateListener;
import io.odpf.stencil.config.StencilConfig;

public class StencilUtils {
    public static StencilConfig getStencilConfig(
            OdpfSinkConfig sinkConfig,
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

    public static StencilConfig getStencilConfig(OdpfSinkConfig config, StatsDClient statsDClient) {
        return getStencilConfig(config, statsDClient, null);
    }
}
