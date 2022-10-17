package io.odpf.depot.bigtable.model;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.odpf.depot.error.ErrorInfo;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class BigTableRecord {
    private final RowMutationEntry rowMutationEntry;
    private final long index;
    private final ErrorInfo errorInfo;
    private final boolean valid;

    @Override
    public String toString() {
        return String.format("RowMutationEntry: %s ValidRecord=%s", rowMutationEntry != null ? rowMutationEntry.toString() : "NULL", valid);
    }
}
