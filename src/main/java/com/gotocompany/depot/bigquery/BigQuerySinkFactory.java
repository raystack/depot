package com.gotocompany.depot.bigquery;

import com.gotocompany.depot.bigquery.client.BigQueryClient;
import com.gotocompany.depot.bigquery.client.BigQueryRow;
import com.gotocompany.depot.bigquery.client.BigQueryRowWithInsertId;
import com.gotocompany.depot.bigquery.client.BigQueryRowWithoutInsertId;
import com.gotocompany.depot.bigquery.converter.MessageRecordConverterCache;
import com.gotocompany.depot.bigquery.handler.ErrorHandler;
import com.gotocompany.depot.bigquery.handler.ErrorHandlerFactory;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClient;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageClientFactory;
import com.gotocompany.depot.bigquery.storage.BigQueryStorageResponseParser;
import com.gotocompany.depot.bigquery.storage.BigQueryWriter;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterFactory;
import com.gotocompany.depot.bigquery.storage.BigQueryWriterUtils;
import com.timgroup.statsd.NoOpStatsDClient;
import com.gotocompany.depot.Sink;
import com.gotocompany.depot.config.BigQuerySinkConfig;
import com.gotocompany.depot.message.MessageParser;
import com.gotocompany.depot.message.MessageParserFactory;
import com.gotocompany.depot.metrics.BigQueryMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import com.gotocompany.depot.metrics.StatsDReporter;
import com.gotocompany.depot.stencil.DepotStencilUpdateListener;
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

    public BigQuerySinkFactory(Map<String, String> env, StatsDReporter statsDReporter, Function<Map<String, Object>, String> rowIDCreator) {
        this(ConfigFactory.create(BigQuerySinkConfig.class, env), statsDReporter, rowIDCreator);
    }

    public BigQuerySinkFactory(BigQuerySinkConfig sinkConfig, StatsDReporter statsDReporter, Function<Map<String, Object>, String> rowIDCreator) {
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
            this.bigQueryClient = new BigQueryClient(sinkConfig, bigQueryMetrics, new Instrumentation(statsDReporter, BigQueryClient.class));
            this.converterCache = new MessageRecordConverterCache();
            this.errorHandler = ErrorHandlerFactory.create(sinkConfig, bigQueryClient, statsDReporter);
            DepotStencilUpdateListener depotStencilUpdateListener = BigqueryStencilUpdateListenerFactory.create(sinkConfig, bigQueryClient, converterCache, statsDReporter);
            MessageParser messageParser = MessageParserFactory.getParser(sinkConfig, statsDReporter, depotStencilUpdateListener);
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
                bigQueryStorageClient = BigQueryStorageClientFactory.createBigQueryStorageClient(sinkConfig, messageParser, bigQueryWriter);
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
                    bigQueryMetrics,
                    new Instrumentation(statsDReporter, BigQueryStorageAPISink.class),
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
