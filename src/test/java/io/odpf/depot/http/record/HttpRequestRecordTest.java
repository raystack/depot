package io.odpf.depot.http.record;

import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

public class HttpRequestRecordTest {

    @Mock
    private HttpEntityEnclosingRequestBase httpRequest;

    @Test
    public void shouldGetRecordIndex() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        Assert.assertEquals(new Long(0), httpRequestRecord.getIndex());
    }

    @Test
    public void shouldGetRecordErrorInfo() {
        ErrorInfo errorInfo = new ErrorInfo(new Exception(""), ErrorType.DEFAULT_ERROR);
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, errorInfo, true);
        Assert.assertEquals(errorInfo, httpRequestRecord.getErrorInfo());
    }

    @Test
    public void shouldGetValidRecord() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, true);
        Assert.assertTrue(httpRequestRecord.isValid());
    }

    @Test
    public void shouldGetInvalidRecord() {
        HttpRequestRecord httpRequestRecord = new HttpRequestRecord(httpRequest, 0L, null, false);
        Assert.assertFalse(httpRequestRecord.isValid());
    }
}
