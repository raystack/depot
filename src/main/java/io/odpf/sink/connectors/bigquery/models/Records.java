package io.odpf.sink.connectors.bigquery.models;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

@AllArgsConstructor
@Getter
public class Records {
    private final List<Record> validRecords;
    private final List<Record> invalidRecords;
}
