package com.gotocompany.depot.log;

import com.gotocompany.depot.config.SinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
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
