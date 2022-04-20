package io.odpf.sink.connectors.bigquery;

import io.odpf.sink.connectors.OdpfSink;
import io.odpf.sink.connectors.bigquery.converter.MessageRecordConverterCache;
import io.odpf.sink.connectors.bigquery.handler.BigQueryClient;
import io.odpf.sink.connectors.bigquery.handler.BigQueryRow;
import io.odpf.sink.connectors.bigquery.handler.BigQueryRowWithInsertId;
import io.odpf.sink.connectors.bigquery.handler.BigQueryRowWithoutInsertId;
import io.odpf.sink.connectors.bigquery.proto.OdpfStencilUpdateListener;
import io.odpf.sink.connectors.bigquery.proto.ProtoUpdateListener;
import io.odpf.sink.connectors.config.BigQuerySinkConfig;
import io.odpf.sink.connectors.message.OdpfMessage;
import io.odpf.sink.connectors.message.OdpfMessageParser;
import io.odpf.sink.connectors.message.OdpfMessageParserFactory;
import io.odpf.sink.connectors.metrics.BigQueryMetrics;
import io.odpf.sink.connectors.metrics.Instrumentation;
import io.odpf.sink.connectors.metrics.StatsDReporter;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class BigQuerySinkFactory {

    private final StatsDReporter statsDReporter;
    private BigQueryClient bigQueryClient;
    private MessageRecordConverterCache recordConverterWrapper;
    private BigQueryRow rowCreator;
    private final Function<Map<String, Object>, String> rowIDCreator;
    private final Map<String, String> config;
    private BigQueryMetrics bigQueryMetrics;

    public BigQuerySinkFactory(Map<String, String> env, StatsDReporter statsDReporter, Function<Map<String, Object>, String> rowIDCreator) {
        this.config = env;
        this.rowIDCreator = rowIDCreator;
        this.statsDReporter = statsDReporter;
    }

    public void init() {
        BigQuerySinkConfig sinkConfig = ConfigFactory.create(BigQuerySinkConfig.class, config);
        try {
            this.bigQueryClient = new BigQueryClient(sinkConfig, bigQueryMetrics, new Instrumentation(statsDReporter, BigQueryClient.class));
            this.bigQueryMetrics = new BigQueryMetrics(sinkConfig);
            this.recordConverterWrapper = new MessageRecordConverterCache();
            OdpfStencilUpdateListener protoUpdateListener = new ProtoUpdateListener(sinkConfig, bigQueryClient, recordConverterWrapper);
            OdpfMessageParser odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter, protoUpdateListener);
            protoUpdateListener.setMessageParser(odpfMessageParser);
            if (sinkConfig.isRowInsertIdEnabled()) {
                this.rowCreator = new BigQueryRowWithInsertId(rowIDCreator);
            } else {
                this.rowCreator = new BigQueryRowWithoutInsertId();
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception occurred while creating sink", e);
        }
    }

    public OdpfSink create() {
        return new BigQuerySink(
                bigQueryClient,
                recordConverterWrapper,
                rowCreator,
                bigQueryMetrics,
                new Instrumentation(statsDReporter, BigQuerySink.class));
    }
}
