package io.odpf.sink.connectors.bigquery.models;

import io.odpf.sink.connectors.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
public class Record {
    private final Map<String, Object> metadata;
    private final Map<String, Object> columns;
    private final long index;
    private final ErrorInfo errorInfo;
}
