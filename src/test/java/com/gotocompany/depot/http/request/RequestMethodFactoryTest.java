package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;

import java.net.URI;

public class RequestMethodFactoryTest {

    @Mock
    private URI uri;

    @Test
    public void shouldReturnPutRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.PUT);
        Assert.assertEquals("PUT", request.getMethod());

    }

    @Test
    public void shouldReturnPostRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.POST);
        Assert.assertEquals("POST", request.getMethod());
    }

    @Test
    public void shouldReturnPatchRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.PATCH);
        Assert.assertEquals("PATCH", request.getMethod());
    }

    @Test
    public void shouldReturnDeleteRequestMethod() {
        HttpEntityEnclosingRequestBase request = RequestMethodFactory.create(uri, HttpRequestMethodType.DELETE);
        Assert.assertEquals("DELETE", request.getMethod());
    }
}
