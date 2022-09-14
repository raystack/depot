package io.odpf.depot.bigtable;

import com.timgroup.statsd.NoOpStatsDClient;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigtable.client.BigTableClient;
import io.odpf.depot.bigtable.parser.BigTableRecordParser;
import io.odpf.depot.bigtable.parser.BigTableRowKeyParser;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.metrics.StatsDReporter;

import java.io.IOException;

public class BigTableSinkFactory {
    private final BigTableSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private BigTableClient bigTableClient;
    private BigTableRecordParser bigTableRecordParser;

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }


    public void init() {
        try {
            OdpfMessageParser odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
            BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser();
            this.bigTableClient = new BigTableClient(sinkConfig);
            this.bigTableRecordParser = new BigTableRecordParser(sinkConfig, odpfMessageParser, bigTableRowKeyParser);
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception occurred while creating sink", e);
        }
    }

    public OdpfSink create() {
        return new BigTableSink(
                bigTableClient,
                bigTableRecordParser);
    }
}
