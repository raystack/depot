package io.odpf.sink.connectors.metrics;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import io.odpf.sink.connectors.config.MetricsConfig;
import lombok.extern.slf4j.Slf4j;

/**
 * StatsDReporterFactory
 * <p>
 * Create statsDReporter Instance.
 */
@Slf4j
public class StatsDReporterBuilder {

    private MetricsConfig metricsConfig;
    private String[] extraTags;

    private static <T> T[] append(T[] arr, T lastElement) {
        final int length = arr.length;
        arr = java.util.Arrays.copyOf(arr, length + 1);
        arr[length] = lastElement;
        return arr;
    }

    public static StatsDReporterBuilder builder() {
        return new StatsDReporterBuilder();
    }

    public StatsDReporterBuilder withMetricConfig(MetricsConfig config) {
        this.metricsConfig = config;
        return this;
    }

    public StatsDReporterBuilder withExtraTags(String... tags) {
        this.extraTags = tags;
        return this;
    }

    private static <T> T[] append(T[] arr, T[] second) {
        final int length = arr.length;
        arr = java.util.Arrays.copyOf(arr, length + second.length);
        System.arraycopy(second, 0, arr, length, second.length);
        return arr;
    }

    public StatsDReporter build() {
        StatsDClient statsDClient = buildStatsDClient();
        return new StatsDReporter(statsDClient, append(metricsConfig.getMetricStatsDTags().split(","), extraTags));
    }

    private StatsDClient buildStatsDClient() {
        StatsDClient statsDClient;
        try {
            statsDClient = new NonBlockingStatsDClientBuilder()
                    .hostname(metricsConfig.getMetricStatsDHost())
                    .port(metricsConfig.getMetricStatsDPort())
                    .build();
            log.info("NonBlocking StatsD client connection established");
        } catch (Exception e) {
            log.warn("Exception on creating StatsD client, disabling StatsD and Audit client", e);
            log.warn("Application is running without collecting any metrics!!!!!!!!");
            statsDClient = new NoOpStatsDClient();
        }
        return statsDClient;
    }
}
