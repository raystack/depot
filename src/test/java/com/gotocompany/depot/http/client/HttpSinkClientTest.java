package com.gotocompany.depot.http.client;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.HttpSinkMetrics;
import com.gotocompany.depot.metrics.Instrumentation;
import org.aeonbits.owner.ConfigFactory;
import org.apache.http.client.HttpClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkClientTest {

    @Mock
    private HttpClient client;

    private HttpSinkMetrics httpSinkMetrics;

    @Mock
    private Instrumentation instrumentation;

    @Before
    public void setUp() throws IOException {
        System.setProperty("SINK_METRICS_APPLICATION_PREFIX", "xyz_");
        HttpSinkConfig sinkConfig = ConfigFactory.create(HttpSinkConfig.class, System.getProperties());
        httpSinkMetrics = new HttpSinkMetrics(sinkConfig);

    }

    @Test
    public void shouldSendRecords() throws IOException {
        HttpSinkClient sinkClient = new HttpSinkClient(client, httpSinkMetrics, instrumentation);

        List<HttpRequestRecord> requestRecords = new ArrayList<HttpRequestRecord>() {{
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
        }};

        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
        }};

        IntStream.range(0, requestRecords.size()).forEach(
                index -> {
                    try {
                        when(requestRecords.get(index).send(client)).thenReturn(responses.get(index));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        IntStream.range(0, responses.size()).forEach(
                index -> when(responses.get(index).getResponseCode()).thenReturn("200")
        );
        List<HttpSinkResponse> actualResponses = sinkClient.send(requestRecords);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }

    @Test
    public void shouldCaptureStatusCodeCount() throws IOException {
        HttpSinkClient sinkClient = new HttpSinkClient(client, httpSinkMetrics, instrumentation);

        List<HttpRequestRecord> requestRecords = new ArrayList<HttpRequestRecord>() {{
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
            add(Mockito.mock(HttpRequestRecord.class));
        }};

        List<HttpSinkResponse> responses = new ArrayList<HttpSinkResponse>() {{
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
            add(Mockito.mock(HttpSinkResponse.class));
        }};

        IntStream.range(0, requestRecords.size()).forEach(
                index -> {
                    try {
                        when(requestRecords.get(index).send(client)).thenReturn(responses.get(index));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        IntStream.range(0, responses.size()).forEach(
                index -> when(responses.get(index).getResponseCode()).thenReturn("200")
        );
        sinkClient.send(requestRecords);
        verify(instrumentation, times(5)).captureCount("xyz_sink_http_response_code_total", 1L, "status_code=200");

    }
}
