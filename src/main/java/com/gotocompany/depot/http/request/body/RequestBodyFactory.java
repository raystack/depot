package com.gotocompany.depot.http.request.body;

import com.gotocompany.depot.config.HttpSinkConfig;
import com.gotocompany.depot.http.enums.HttpRequestBodyType;

public class RequestBodyFactory {

    public static RequestBody create(HttpSinkConfig config) {
        HttpRequestBodyType bodyType = config.getRequestBodyType();
        switch (bodyType) {
            case JSON:
                return new JsonBody(config);
            case MESSAGE:
                return new MessageBody(config);
            case TEMPLATIZED_JSON:
                return new TemplatizedJsonBody(config);
            default:
                return new RawBody(config);
        }
    }
}
