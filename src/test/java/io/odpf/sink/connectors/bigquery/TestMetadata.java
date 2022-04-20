package io.odpf.sink.connectors.bigquery;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.Instant;

@EqualsAndHashCode
@ToString
@Getter
@AllArgsConstructor
public class TestMetadata {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final long loadTime;
}
