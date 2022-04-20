package io.odpf.sink.connectors.log;

import io.odpf.sink.connectors.OdpfSink;
import io.odpf.sink.connectors.config.OdpfSinkConfig;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.message.OdpfMessageParserFactory;
import io.odpf.sink.connectors.metrics.Instrumentation;
import io.odpf.sink.connectors.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;

import java.util.Map;

public class LogSinkFactory {

    private final StatsDReporter statsDReporter;
    private final Map<String, String> config;
    private OdpfMessageParser odpfMessageParser;
    private OdpfSinkConfig sinkConfig;

    public LogSinkFactory(Map<String, String> env, StatsDReporter statsDReporter) {
        this.config = env;
        this.statsDReporter = statsDReporter;
    }

    public void init() {
        this.sinkConfig = ConfigFactory.create(OdpfSinkConfig.class, config);
        this.odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
    }

    public OdpfSink create() {
        return new LogSink(sinkConfig, odpfMessageParser, new Instrumentation(statsDReporter, LogSink.class));
    }
}
