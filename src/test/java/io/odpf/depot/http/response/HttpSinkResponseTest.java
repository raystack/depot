package io.odpf.depot.http.response;

import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

public class HttpSinkResponseTest {

    private HttpSinkResponse httpSinkResponse;

    @Mock
    private HttpResponse response;

    @Test
    public void shouldReportWhenFailed() {
        httpSinkResponse = new HttpSinkResponse(response, false);
        Assert.assertFalse(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
    }

    @Test
    public void shouldReportWhenSuccess() {
        httpSinkResponse = new HttpSinkResponse(response, true);
        Assert.assertTrue(httpSinkResponse.isFailed());
        Assert.assertEquals(response, httpSinkResponse.getResponse());
    }
}
