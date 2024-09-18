package org.raystack.depot.metrics;

import com.timgroup.statsd.StatsDClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatsDReporter implements Closeable {

    private final StatsDClient client;
    private final boolean tagsNativeFormatEnabled;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatsDReporter.class);
    private final String[] globalTags;

    public StatsDReporter(StatsDClient client, Boolean tagsNativeFormatEnabled, String... globalTags) {
        this.client = client;
        this.tagsNativeFormatEnabled = tagsNativeFormatEnabled;
        this.globalTags = globalTags;
    }

    public StatsDReporter(StatsDClient client, String... globalTags) {
        this.client = client;
        this.tagsNativeFormatEnabled = false;
        this.globalTags = globalTags;
    }

    public StatsDClient getClient() {
        return client;
    }

    public void captureCount(String metric, Long delta, String... tags) {
        client.count(getMetrics(metric, tags), delta, getTags(tags));
    }

    public void captureHistogram(String metric, long delta, String... tags) {
        client.time(getMetrics(metric, tags), delta, getTags(tags));
    }

    public void captureDurationSince(String metric, Instant startTime, String... tags) {
        client.recordExecutionTime(getMetrics(metric, tags), Duration.between(startTime, Instant.now()).toMillis(), getTags(tags));
    }

    public void captureDuration(String metric, long duration, String... tags) {
        client.recordExecutionTime(getMetrics(metric, tags), duration, getTags(tags));
    }

    public void gauge(String metric, Integer value, String... tags) {
        client.gauge(getMetrics(metric, tags), value, getTags(tags));
    }

    public void increment(String metric, String... tags) {
        captureCount(metric, 1L, getTags(tags));
    }

    public void recordEvent(String metric, String eventName, String... tags) {
        client.recordSetValue(getMetrics(metric, tags), eventName, getTags(tags));
    }

    private String[] getTags(String[] tags) {
        if (!this.tagsNativeFormatEnabled) {
            return null;
        }
        List<String> list = Arrays.stream(tags).map(s -> s.replaceAll("=", ":")).collect(Collectors.toList());
        list.addAll(Arrays.asList(this.getGlobalTags().split(",")));
        return list.toArray(new String[0]);
    }

    private String getMetrics(String metric, String... tags) {
        return this.tagsNativeFormatEnabled ? metric : withTags(metric, tags);
    }

    private String getGlobalTags() {
        if (this.tagsNativeFormatEnabled) {
            return String.join(",", this.globalTags).replaceAll("=", ":");
        }
        return String.join(",", this.globalTags).replaceAll(":", "=");
    }

    private String withTags(String metric, String... tags) {
        return Stream.concat(
                        Stream.of(metric + "," + this.getGlobalTags()),
                        tags == null ? Stream.empty() : Arrays.stream(tags)
                )
                .collect(Collectors.joining(","));
    }


    @Override
    public void close() throws IOException {
        LOGGER.info("StatsD connection closed");
        client.stop();
    }

}
