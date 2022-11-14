package io.odpf.depot.http.request.builder;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.gradle.internal.impldep.org.junit.Assert.assertEquals;

public class UriBuilderTest {

    @Test
    public void shouldReturnURIInstanceBasedOnBaseUrl() throws URISyntaxException {
        UriBuilder uriBuilder = new UriBuilder("http://dummy.com");
        assertEquals(new URI("http://dummy.com"), uriBuilder.build(Collections.emptyMap()));
    }
}
