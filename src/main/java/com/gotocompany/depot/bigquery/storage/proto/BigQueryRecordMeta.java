package com.gotocompany.depot.bigquery.storage.proto;

import com.gotocompany.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BigQueryRecordMeta {
    private final long inputIndex;
    private final ErrorInfo errorInfo;
    private final boolean isValid;
}
