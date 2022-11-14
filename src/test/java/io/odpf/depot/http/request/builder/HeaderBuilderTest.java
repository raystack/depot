package io.odpf.depot.http.request.builder;

import io.odpf.depot.config.HttpSinkConfig;
import org.aeonbits.owner.ConfigFactory;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HeaderBuilderTest {

    private final Map<String, String> configuration = new HashMap<>();

    @Mock
    private HttpSinkConfig config;

    @Test
    public void shouldGenerateBaseHeader() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(config.getSinkHttpHeaders());

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    @Test
    public void shouldHandleMultipleHeader() {
        configuration.put("SINK_HTTP_HEADERS", "Authorization:auth_token,Accept:text/plain");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(config.getSinkHttpHeaders());

        Map<String, String> header = headerBuilder.build();
        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        configuration.put("SINK_HTTP_HEADERS", "");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(config.getSinkHttpHeaders());

        headerBuilder.build();
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowErrorIfHeaderConfigIsInvalid() {
        configuration.put("SINK_HTTP_HEADERS", "content-type:json,header_key;header_value,key:,:value");
        config = ConfigFactory.create(HttpSinkConfig.class, configuration);
        HeaderBuilder headerBuilder = new HeaderBuilder(config.getSinkHttpHeaders());

        headerBuilder.build();
    }
}
