package org.raystack.depot.bigquery;

import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.bigquery.handler.ErrorHandler;
import org.raystack.depot.bigquery.handler.ErrorHandlerFactory;
import org.raystack.depot.message.OdpfMessageParser;
import org.raystack.depot.message.OdpfMessageParserFactory;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.OdpfStencilUpdateListener;
import org.raystack.depot.OdpfSink;
import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.client.BigQueryRow;
import org.raystack.depot.bigquery.client.BigQueryRowWithInsertId;
import org.raystack.depot.bigquery.client.BigQueryRowWithoutInsertId;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class BigQuerySinkFactory {

    private final StatsDReporter statsDReporter;
    private BigQueryClient bigQueryClient;
    private BigQueryRow rowCreator;
    private final Function<Map<String, Object>, String> rowIDCreator;
    private BigQueryMetrics bigQueryMetrics;
    private ErrorHandler errorHandler;
    private MessageRecordConverterCache converterCache;
    private final BigQuerySinkConfig sinkConfig;

    public BigQuerySinkFactory(Map<String, String> env, StatsDReporter statsDReporter,
            Function<Map<String, Object>, String> rowIDCreator) {
        this(ConfigFactory.create(BigQuerySinkConfig.class, env), statsDReporter, rowIDCreator);
    }

    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, StatsDReporter statsDReporter,
            Function<Map<String, Object>, String> rowIDCreator) {
        this.sinkConfig = sinkConfig;
        this.rowIDCreator = rowIDCreator;
        this.statsDReporter = statsDReporter;
    }

    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), null);
    }

    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this(sinkConfig, statsDReporter, null);
    }

    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, Function<Map<String, Object>, String> rowIDCreator) {
        this(sinkConfig, new StatsDReporter(new NoOpStatsDClient()), rowIDCreator);
    }

    public void init() {
        try {
            this.bigQueryMetrics = new BigQueryMetrics(sinkConfig);
            this.bigQueryClient = new BigQueryClient(sinkConfig, bigQueryMetrics,
                    new Instrumentation(statsDReporter, BigQueryClient.class));
            this.converterCache = new MessageRecordConverterCache();
            this.errorHandler = ErrorHandlerFactory.create(sinkConfig, bigQueryClient, statsDReporter);
            OdpfStencilUpdateListener raystackStencilUpdateListener = BigqueryStencilUpdateListenerFactory
                    .create(sinkConfig, bigQueryClient, converterCache, statsDReporter);
            OdpfMessageParser raystackMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter,
                    raystackStencilUpdateListener);
            raystackStencilUpdateListener.setOdpfMessageParser(raystackMessageParser);
            raystackStencilUpdateListener.updateSchema();

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
                converterCache,
                rowCreator,
                bigQueryMetrics,
                new Instrumentation(statsDReporter, BigQuerySink.class),
                errorHandler);
    }
}
