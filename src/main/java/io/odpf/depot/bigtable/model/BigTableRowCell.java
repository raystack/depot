package io.odpf.depot.bigtable.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BigTableRowCell {
    private final String columnFamily;
    private final String qualifier;
    private final String value;
}
