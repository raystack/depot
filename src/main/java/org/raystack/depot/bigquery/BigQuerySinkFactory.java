package org.raystack.depot.bigquery;

import org.raystack.depot.bigquery.client.BigQueryClient;
import org.raystack.depot.bigquery.client.BigQueryRow;
import org.raystack.depot.bigquery.client.BigQueryRowWithInsertId;
import org.raystack.depot.bigquery.client.BigQueryRowWithoutInsertId;
import org.raystack.depot.bigquery.converter.MessageRecordConverterCache;
import org.raystack.depot.bigquery.handler.ErrorHandler;
import org.raystack.depot.bigquery.handler.ErrorHandlerFactory;
import org.raystack.depot.bigquery.storage.BigQueryStorageClient;
import org.raystack.depot.bigquery.storage.BigQueryStorageClientFactory;
import org.raystack.depot.bigquery.storage.BigQueryStorageResponseParser;
import org.raystack.depot.bigquery.storage.BigQueryWriter;
import org.raystack.depot.bigquery.storage.BigQueryWriterFactory;
import org.raystack.depot.bigquery.storage.BigQueryWriterUtils;
import com.timgroup.statsd.NoOpStatsDClient;
import org.raystack.depot.Sink;
import org.raystack.depot.config.BigQuerySinkConfig;
import org.raystack.depot.message.MessageParser;
import org.raystack.depot.message.MessageParserFactory;
import org.raystack.depot.metrics.BigQueryMetrics;
import org.raystack.depot.metrics.Instrumentation;
import org.raystack.depot.metrics.StatsDReporter;
import org.raystack.depot.stencil.DepotStencilUpdateListener;
import org.aeonbits.owner.ConfigFactory;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class BigQuerySinkFactory {

    private final StatsDReporter statsDReporter;
    private final Function<Map<String, Object>, String> rowIDCreator;
    private final BigQuerySinkConfig sinkConfig;
    private BigQueryClient bigQueryClient;
    private BigQueryRow rowCreator;
    private BigQueryMetrics bigQueryMetrics;
    private ErrorHandler errorHandler;
    private MessageRecordConverterCache converterCache;
    private BigQueryStorageClient bigQueryStorageClient;
    private BigQueryStorageResponseParser responseParser;

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
            DepotStencilUpdateListener depotStencilUpdateListener = BigqueryStencilUpdateListenerFactory
                    .create(sinkConfig, bigQueryClient, converterCache, statsDReporter);
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter,
                    depotStencilUpdateListener);
            depotStencilUpdateListener.setMessageParser(messageParser);
            depotStencilUpdateListener.updateSchema();

            if (sinkConfig.isRowInsertIdEnabled()) {
                this.rowCreator = new BigQueryRowWithInsertId(rowIDCreator);
            } else {
                this.rowCreator = new BigQueryRowWithoutInsertId();
            }
            if (sinkConfig.getSinkBigqueryStorageAPIEnable()) {
                BigQueryWriter bigQueryWriter = BigQueryWriterFactory
                        .createBigQueryWriter(
                                sinkConfig,
                                BigQueryWriterUtils::getBigQueryWriterClient,
                                BigQueryWriterUtils::getCredentialsProvider,
                                BigQueryWriterUtils::getStreamWriter,
                                new Instrumentation(statsDReporter, BigQueryWriter.class),
                                bigQueryMetrics);
                bigQueryWriter.init();
                bigQueryStorageClient = BigQueryStorageClientFactory.createBigQueryStorageClient(sinkConfig,
                        messageParser, bigQueryWriter);
                responseParser = new BigQueryStorageResponseParser(
                        sinkConfig,
                        new Instrumentation(statsDReporter, BigQueryStorageResponseParser.class),
                        bigQueryMetrics);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception occurred while creating sink", e);
        }
    }

    public Sink create() {
        if (sinkConfig.getSinkBigqueryStorageAPIEnable()) {
            return new BigQueryStorageAPISink(
                    bigQueryStorageClient,
                    responseParser);
        } else {
            return new BigQuerySink(
                    bigQueryClient,
                    converterCache,
                    rowCreator,
                    bigQueryMetrics,
                    new Instrumentation(statsDReporter, BigQuerySink.class),
                    errorHandler);
        }
    }
}
