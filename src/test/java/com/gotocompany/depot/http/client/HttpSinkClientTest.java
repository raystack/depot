package com.gotocompany.depot.http.client;

import com.gotocompany.depot.http.record.HttpRequestRecord;
import com.gotocompany.depot.http.response.HttpSinkResponse;
import com.gotocompany.depot.metrics.Instrumentation;
import org.apache.http.client.HttpClient;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.class)
public class HttpSinkClientTest {

    @Mock
    private HttpClient client;

    @Mock
    private Instrumentation instrumentation;

    @Test
    public void shouldSendRecords() throws IOException {
        HttpSinkClient sinkClient = new HttpSinkClient(client, instrumentation);

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
                        Mockito.when(requestRecords.get(index).send(client)).thenReturn(responses.get(index));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
        List<HttpSinkResponse> actualResponses = sinkClient.send(requestRecords);
        IntStream.range(0, actualResponses.size()).forEach(
                index -> {
                    Assert.assertEquals(responses.get(index), actualResponses.get(index));
                }
        );
    }
}
