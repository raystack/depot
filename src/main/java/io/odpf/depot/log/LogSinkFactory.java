package org.raystack.depot.log;

import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.message.OdpfMessageParserFactory;
import org.raystack.depot.OdpfSink;
import org.raystack.depot.config.OdpfSinkConfig;
import org.raystack.depot.message.OdpfMessageParser;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class LogSinkFactory {

    private final StatsDReporter statsDReporter;
    private OdpfMessageParser raystackMessageParser;
    private final OdpfSinkConfig sinkConfig;

    public LogSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this(ConfigFactory.create(OdpfSinkConfig.class, env), statsDReporter);
    }

    public LogSinkFactory(OdpfSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public LogSinkFactory(OdpfSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }

    public void init() {
        this.raystackMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
    }

    public OdpfSink create() {
        return new LogSink(sinkConfig, raystackMessageParser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
