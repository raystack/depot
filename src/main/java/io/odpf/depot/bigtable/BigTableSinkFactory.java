package io.odpf.depot.bigtable;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.config.BigTableSinkConfig;
import io.odpf.depot.message.OdpfMessageParser;
import io.odpf.depot.message.OdpfMessageParserFactory;
import io.odpf.depot.metrics.StatsDReporter;

import java.io.FileInputStream;
import java.io.IOException;

public class BigTableSinkFactory {
    private BigtableDataClient bigtableDataClient;
    private OdpfMessageParser odpfMessageParser;
    private final BigTableSinkConfig sinkConfig;
    private final StatsDReporter statsDReporter;

    public BigTableSinkFactory(BigTableSinkConfig sinkConfig, StatsDReporter statsDReporter) {
        this.sinkConfig = sinkConfig;
        this.statsDReporter = statsDReporter;
    }

    public void init() {
        try {
            this.odpfMessageParser = OdpfMessageParserFactory.getParser(sinkConfig, statsDReporter);
            BigtableDataSettings settings = BigtableDataSettings.newBuilder()
                    .setCredentialsProvider(FixedCredentialsProvider.create(GoogleCredentials.fromStream(new FileInputStream(sinkConfig.getBigTableCredentialPath()))))
                    .setProjectId(sinkConfig.getGCloudProjectID())
                    .setInstanceId(sinkConfig.getBigtableInstanceId()).build();
            this.bigtableDataClient = BigtableDataClient.create(settings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public OdpfSink create() {
        return new BigTableSink(
                bigtableDataClient,
                sinkConfig,
                odpfMessageParser);
    }
}
