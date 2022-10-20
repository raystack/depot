package io.odpf.depot.http.request.body;

import io.odpf.depot.config.HttpSinkConfig;
import io.odpf.depot.http.enums.HttpRequestBodyType;

public class RequestBodyFactory {

    public static RequestBody create(HttpSinkConfig config) {
        HttpRequestBodyType bodyType = config.getRequestBodyType();
        switch (bodyType) {
            case JSON:
                return new JsonBody();
            case MESSAGE:
                return new MessageBody();
            case TEMPLATIZED_JSON:
                return new TemplatizedJsonBody();
            default:
                return new RawBody(config);
        }
    }
}
