package io.odpf.depot.config;

import org.aeonbits.owner.Config;

public interface MetricsConfig extends Config {

    @Config.Key("METRIC_STATSD_HOST")
    @DefaultValue("localhost")
    String getMetricStatsDHost();

    @Config.Key("METRIC_STATSD_PORT")
    @DefaultValue("8125")
    Integer getMetricStatsDPort();

    @Config.Key("METRIC_STATSD_TAGS")
    @DefaultValue("")
    String getMetricStatsDTags();
}
