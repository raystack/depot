package com.gotocompany.depot.bigquery.models;

import com.gotocompany.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Map;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@Builder
@ToString
public class Record {
    private final Map<String, Object> metadata;
    private final Map<String, Object> columns;
    private final long index;
    private final ErrorInfo errorInfo;
}
