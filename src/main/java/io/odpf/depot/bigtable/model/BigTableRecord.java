package io.odpf.depot.bigtable.model;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.odpf.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@AllArgsConstructor
@Getter
public class BigTableRecord {
    private final RowMutationEntry rowMutationEntry;
    private final long index;
    private final ErrorInfo errorInfo;
    private final Map<String, Object> metadata;

    public boolean isValid() {
        return errorInfo == null;
    }
}
