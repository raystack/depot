package org.raystack.depot.log;

import org.raystack.depot.config.SinkConfig;
import org.raystack.depot.message.MessageParser;
import org.raystack.depot.message.MessageParserFactory;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.Sink;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class LogSinkFactory {

    private final StatsDReporter statsDReporter;
    private MessageParser messageParser;
    private final SinkConfig sinkConfig;

    public LogSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this(ConfigFactory.create(SinkConfig.class, env), statsDReporter);
    }

    public LogSinkFactory(SinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public LogSinkFactory(SinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }

    public void init() {
        this.messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
    }

    public Sink create() {
        return new LogSink(sinkConfig, messageParser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
