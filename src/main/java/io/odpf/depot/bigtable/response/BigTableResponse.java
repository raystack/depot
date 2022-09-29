package io.odpf.depot.bigtable.response;

import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import lombok.Getter;

import java.util.List;

@Getter
public class BigTableResponse {
    private final List<MutateRowsException.FailedMutation> failedMutations;

    public BigTableResponse(List<MutateRowsException.FailedMutation> failedMutations) {
        this.failedMutations = failedMutations;
    }

    public boolean hasErrors() {
        return !failedMutations.isEmpty();
    }
}