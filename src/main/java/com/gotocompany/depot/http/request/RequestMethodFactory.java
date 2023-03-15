package com.gotocompany.depot.http.request;

import com.gotocompany.depot.http.enums.HttpRequestMethodType;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;

import java.net.URI;

public class RequestMethodFactory {

    public static HttpEntityEnclosingRequestBase create(URI uri, HttpRequestMethodType method) {
        switch (method) {
            case POST:
                return new HttpPost(uri);
            case PATCH:
                return new HttpPatch(uri);
            default:
                return new HttpPut(uri);
        }
    }
}
