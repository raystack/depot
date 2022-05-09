package io.odpf.depot.bigquery.models;

import io.odpf.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Builder
public class Record {
    private final Map<String, Object> metadata;
    private final Map<String, Object> columns;
    private final long index;
    private final ErrorInfo errorInfo;
}
