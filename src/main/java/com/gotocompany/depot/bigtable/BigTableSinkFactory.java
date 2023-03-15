package com.gotocompany.depot.bigtable;

import com.gotocompany.depot.bigtable.client.BigTableClient;
import com.gotocompany.depot.bigtable.model.BigTableSchema;
import com.gotocompany.depot.bigtable.parser.BigTableRecordParser;
import com.gotocompany.depot.bigtable.parser.BigTableRowKeyParser;
import com.gotocompany.depot.exception.ConfigurationException;
import com.gotocompany.depot.exception.InvalidTemplateException;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.common.Template;
import com.gotocompany.depot.common.Tuple;
import com.gotocompany.depot.config.BigTableSinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.message.MessageSchema;
import com.gotocompany.depot.message.SinkConnectorSchemaMessageMode;
import com.gotocompany.depot.metrics.BigTableMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.utils.MessageConfigUtils;

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
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter);
            MessageSchema schema = messageParser.getSchema(modeAndSchema.getSecond());

            Template keyTemplate = new Template(sinkConfig.getRowKeyTemplate());
            BigTableRowKeyParser bigTableRowKeyParser = new BigTableRowKeyParser(keyTemplate, schema);
            bigTableRecordParser = new BigTableRecordParser(
                    messageParser,
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
