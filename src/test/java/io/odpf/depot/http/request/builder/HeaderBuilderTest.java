package io.odpf.depot.http.request.builder;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HeaderBuilderTest {

    @Test
    public void shouldGenerateBaseHeader() {
        String headerConfig = "content-type:json";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        assertEquals("json", headerBuilder.build().get("content-type"));
    }

    @Test
    public void shouldHandleMultipleHeader() {
        String headerConfig = "Authorization:auth_token,Accept:text/plain";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        Map<String, String> header = headerBuilder.build();
        assertEquals("auth_token", header.get("Authorization"));
        assertEquals("text/plain", header.get("Accept"));
    }

    @Test
    public void shouldNotThrowNullPointerExceptionWhenHeaderConfigEmpty() {
        String headerConfig = "";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        headerBuilder.build();
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void shouldThrowErrorIfHeaderConfigIsInvalid() {
        String headerConfig = "content-type:json,header_key;header_value,key:,:value";
        HeaderBuilder headerBuilder = new HeaderBuilder(headerConfig);

        headerBuilder.build();
    }
}
