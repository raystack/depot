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
import io.odpf.depot.exception.InvalidTemplateException;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.message.OdpfMessageSchema;
import io.odpf.depot.message.SinkConnectorSchemaMessageMode;
import io.odpf.depot.metrics.BigTableMetrics;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.utils.MessageConfigUtils;

import java.io.IOException;

public class BigTableSinkFactory {
    private final BigTableSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;
    private BigTableClient bigTableClient;
    private BigTableRecordParser bigTableRecordParser;
    private BigTableMetrics bigtableMetrics;

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()));
    }


    public void init() {
        try {
            Instrumentation instrumentation = new Instrumentation(statsDReporter, BigTableSinkFactory.class);
            String bigtableConfig = String.format("\n\tbigtable.gcloud.project = %s\n\tbigtable.instance = %s\n\tbigtable.table = %s"
                            + "\n\tbigtable.credential.path = %s\n\tbigtable.row.key.template = %s\n\tbigtable.column.family.mapping = %s\n\t",
                    sinkConfig.getGCloudProjectID(),
                    sinkConfig.getInstanceId(),
                    sinkConfig.getTableId(),
                    sinkConfig.getCredentialPath(),
                    sinkConfig.getRowKeyTemplate(),
                    sinkConfig.getColumnFamilyMapping());

            instrumentation.logInfo(bigtableConfig);
            BigTableSchema bigtableSchema = new BigTableSchema(sinkConfig.getColumnFamilyMapping());
            bigtableMetrics = new BigTableMetrics(sinkConfig);
            bigTableClient = new BigTableClient(sinkConfig, bigtableSchema, bigtableMetrics, new Instrumentation(statsDReporter, BigTableClient.class));
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
            instrumentation.logInfo("Connection to bigtable established successfully");
        } catch (IOException | InvalidTemplateException e) {
            throw new ConfigurationException("Exception occurred while creating sink", e);
        }
    }

    public OdpfSink create() {
        return new BigTableSink(
                bigTableClient,
                bigTableRecordParser,
                bigtableMetrics,
                new Instrumentation(statsDReporter, BigTableSink.class));
    }
}
