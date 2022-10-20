package io.odpf.depot.bigtable.response;

import com.google.api.gax.rpc.ErrorDetails;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.MutateRowsException;
import lombok.Getter;

import java.util.List;

@Getter
public class BigTableResponse {
    private final List<MutateRowsException.FailedMutation> failedMutations;
    private final ErrorDetails errorDetails;
    private final StatusCode statusCode;
    private final String reason;

    public BigTableResponse(MutateRowsException e) {
        failedMutations = e.getFailedMutations();
        errorDetails = e.getErrorDetails();
        statusCode = e.getStatusCode();
        reason = e.getReason();
    }

    public boolean hasErrors() {
        return !failedMutations.isEmpty();
    }
}
