package io.odpf.depot.http.request.builder;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

import static org.gradle.internal.impldep.org.junit.Assert.assertEquals;

public class UriBuilderTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldReturnURIInstanceBasedOnBaseUrl() throws URISyntaxException {
        UriBuilder uriBuilder = new UriBuilder("http://dummy.com   ");
        assertEquals(new URI("http://dummy.com"), uriBuilder.build(Collections.emptyMap()));
    }

    @Test
    public void shouldFailWhenUrlConfigIsEmpty() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template '' is invalid");
        UriBuilder uriBuilder = new UriBuilder("");
        uriBuilder.build(Collections.emptyMap());
    }

    @Test
    public void shouldFailWhenUrlConfigIsNull() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Template 'null' is invalid");
        UriBuilder uriBuilder = new UriBuilder(null);
        uriBuilder.build(Collections.emptyMap());
    }
}
