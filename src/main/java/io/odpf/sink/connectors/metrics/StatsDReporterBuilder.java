package io.odpf.sink.connectors.metrics;

import com.timgroup.statsd.NoOpStatsDClient;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.StatsDClient;
import io.odpf.sink.connectors.config.MetricsConfig;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * StatsDReporterFactory
 * <p>
 * Create statsDReporter Instance.
 */
@Builder
public class StatsDReporterBuilder {

    private MetricsConfig metricsConfig;
    private String[] extraTags;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsDReporterBuilder.class);

    private static <T> T[] append(T[] arr, T lastElement) {
        final int length = arr.length;
        arr = java.util.Arrays.copyOf(arr, length + 1);
        arr[length] = lastElement;
        return arr;
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
            LOGGER.info("NonBlocking StatsD client connection established");
        } catch (Exception e) {
            LOGGER.warn("Exception on creating StatsD client, disabling StatsD and Audit client", e);
            LOGGER.warn("Application is running without collecting any metrics!!!!!!!!");
            statsDClient = new NoOpStatsDClient();
        }
        return statsDClient;
    }
}
