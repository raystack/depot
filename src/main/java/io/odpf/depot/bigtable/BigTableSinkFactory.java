package io.odpf.depot.bigtable;

import com.timgroup.statsd.NoOpStatsDClient;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigtable.client.BigTableClient;
import io.odpf.depot.bigtable.model.BigTableSchema;
import io.odpf.depot.bigtable.parser.BigTableRecordParser;
import io.odpf.depot.bigtable.parser.BigTableRowKeyParser;
import io.odpf.depot.common.Template;
import io.odpf.depot.common.Tuple;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.exception.ConfigurationException;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.utils.MessageConfigUtils;

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
            BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
            bigTableClient = new BigTableClient(sinkConfig, bigtableSchema);
            bigTableClient.validateBigTableSchema();

            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils.getModeAndSchema(sinkConfig);
            OdpfMessageParser odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
            OdpfMessageSchema schema = odpfMessageParser.getSchema(modeAndSchema.getSecond());

            Template keyTemplate = new Template(sinkConfig.getRowKeyTemplate());
            BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(keyTemplate, schema);
            bigTableRecordParser = new BigTableRecordParser(
                    odpfMessageParser,
                    bigTableRowKeyParser,
                    modeAndSchema,
                    schema,
                    bigtableSchema);
        } catch (IOException e) {
            throw new ConfigurationException("Exception occurred while creating sink", e);
        }
    }

    public OdpfSink create() {
        return new BigTableSink(
                bigTableClient,
                bigTableRecordParser);
    }
}
