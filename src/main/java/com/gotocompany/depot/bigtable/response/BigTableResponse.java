package com.gotocompany.depot.bigtable.response;

import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import lombok.Getter;

import java.util.List;

@Getter
public class BigTableResponse {
    private final List<MutateRowsException.FailedMutation> failedMutations;

    public BigTableResponse(MutateRowsException e) {
        failedMutations = e.getFailedMutations();
    }

    public boolean hasErrors() {
        return !failedMutations.isEmpty();
    }

    public int getErrorCount() {
        return failedMutations.size();
    }
}
