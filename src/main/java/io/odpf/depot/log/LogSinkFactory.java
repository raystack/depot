package org.raystack.depot.log;

import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.message.RaystackMessageParserFactory;
import org.raystack.depot.RaystackSink;
import org.raystack.depot.config.RaystackSinkConfig;
import org.raystack.depot.message.RaystackMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class LogSinkFactory {

    private final StatsDReporter statsDReporter;
    private RaystackMessageParser raystackMessageParser;
    private final RaystackSinkConfig sinkConfig;

    public LogSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this(ConfigFactory.create(RaystackSinkConfig.class, env), statsDReporter);
    }

    public LogSinkFactory(RaystackSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public LogSinkFactory(RaystackSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }

    public void init() {
        this.raystackMessageParser = RaystackMessageParserFactory.getParser(sinkConfig, statsDReporter);
    }

    public RaystackSink create() {
        return new LogSink(sinkConfig, raystackMessageParser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
