package io.odpf.depot.log;

import com.timgroup.statsd.NoOpStatsDClient;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.config.OdpfSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class LogSinkFactory {

    private final StatsDReporter statsDReporter;
    private OdpfMessageParser odpfMessageParser;
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
        this.odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
    }

    public OdpfSink create() {
        return new LogSink(sinkConfig, odpfMessageParser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
