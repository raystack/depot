package io.odpf.depot.bigquery;

<<<<<<< HEAD

import io.odpf.depot.OdpfSink;
import io.odpf.depot.bigquery.handler.BigQueryClient;
import io.odpf.depot.bigquery.handler.BigQueryRow;
import io.odpf.depot.bigquery.handler.BigQueryRowWithInsertId;
import io.odpf.depot.bigquery.handler.BigQueryRowWithoutInsertId;
=======
import com.timgroup.statsd.NoOpStatsDClient;
>>>>>>> main
import io.odpf.depot.bigquery.handler.ErrorHandler;
import io.odpf.depot.bigquery.handler.ErrorHandlerFactory;
import io.odpf.depot.bigquery.handler.MessageRecordConverterCache;
import io.odpf.depot.config.BigQuerySinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.metrics.BigQueryMetrics;
import io.odpf.depot.metrics.Instrumentation;
import io.odpf.depot.metrics.StatsDReporter;
import io.odpf.depot.stencil.OdpfStencilUpdateListener;
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
            OdpfStencilUpdateListener odpfStencilUpdateListener = BigqueryStencilUpdateListenerFactory.create(sinkConfig, bigQueryClient, converterCache, statsDReporter);
            OdpfMessageParser odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter, odpfStencilUpdateListener);
            odpfStencilUpdateListener.setOdpfMessageParser(odpfMessageParser);
            odpfStencilUpdateListener.updateSchema();

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
