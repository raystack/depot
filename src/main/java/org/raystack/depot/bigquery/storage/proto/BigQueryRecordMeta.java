package org.raystack.depot.bigquery.storage.proto;

import org.raystack.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BigQueryRecordMeta {
    private final long inputIndex;
    private final ErrorInfo errorInfo;
    private final boolean isValid;
}
