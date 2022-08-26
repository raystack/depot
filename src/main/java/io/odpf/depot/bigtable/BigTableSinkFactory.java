package io.odpf.depot.bigtable;

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
    private BigTableClient bigTableClient;
    private final BigTableSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private BigTableRecordParser bigTableRecordParser;

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public void init() {
        try {
            OdpfMessageParser odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
            BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser();
            this.bigTableClient = new BigTableClient(sinkConfig);
            this.bigTableRecordParser = new BigTableRecordParser(sinkConfig, odpfMessageParser, bigTableRowKeyParser);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public OdpfSink create() {
        return new BigTableSink(
                bigTableClient,
                bigTableRecordParser);
    }
}
