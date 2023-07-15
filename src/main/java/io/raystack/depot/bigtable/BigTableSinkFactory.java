package org.raystack.depot.bigtable;

import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.Sink;
import org.raystack.depot.bigtable.client.BigTableClient;
import org.raystack.depot.bigtable.model.BigTableSchema;
import org.raystack.depot.bigtable.parser.BigTableRecordParser;
import org.raystack.depot.bigtable.parser.BigTableRowKeyParser;
import org.raystack.depot.common.Template;
import org.raystack.depot.common.Tuple;
import org.raystack.depot.config.BigTableSinkConfig;
import org.raystack.depot.exception.ConfigurationException;
import org.raystack.depot.exception.InvalidTemplateException;
import org.raystack.depot.message.MessageParser;
import org.raystack.depot.message.MessageParserFactory;
import org.raystack.depot.message.MessageSchema;
import org.raystack.depot.message.SinkConnectorSchemaMessageMode;
import org.raystack.depot.metrics.BigTableMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.utils.MessageConfigUtils;

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
            String bigtableConfig = String.format(
                    "\n\tbigtable.gcloud.project = %s\n\tbigtable.instance = %s\n\tbigtable.table = %s"
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
            bigTableClient = new BigTableClient(sinkConfig, bigtableSchema, bigtableMetrics,
                    new Instrumentation(statsDReporter, BigTableClient.class));
            bigTableClient.validateBigTableSchema();

            Tuple<SinkConnectorSchemaMessageMode, String> modeAndSchema = MessageConfigUtils
                    .getModeAndSchema(sinkConfig);
            MessageParser raystackMessageParser = MessageParserFactory.getParser(sinkConfig,
                    statsDReporter);
            MessageSchema schema = raystackMessageParser.getSchema(modeAndSchema.getSecond());

            Template keyTemplate = new Template(sinkConfig.getRowKeyTemplate());
            BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(keyTemplate, schema);
            bigTableRecordParser = new BigTableRecordParser(
                    raystackMessageParser,
                    bigTableRowKeyParser,
                    modeAndSchema,
                    schema,
                    bigtableSchema);
            instrumentation.logInfo("Connection to bigtable established successfully");
        } catch (IOException | InvalidTemplateException e) {
            throw new ConfigurationException("Exception occurred while creating sink", e);
        }
    }

    public Sink create() {
        return new BigTableSink(
                bigTableClient,
                bigTableRecordParser,
                bigtableMetrics,
                new Instrumentation(statsDReporter, BigTableSink.class));
    }
}
