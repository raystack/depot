package io.odpf.sink.connectors;

import io.odpf.sink.connectors.error.ErrorInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpfSinkResponse {
    private final Map<Long, List<ErrorInfo>> errors = new HashMap<>();

    /**
     * Returns all errors as a map whose keys are indexes of rows that failed to be pushed.
     * Each failed row index is associated with a non-empty list of {@link ErrorInfo}.
     */
    public Map<Long, List<ErrorInfo>> getErrors() {
        return errors;
    }

    /**
     * Returns errors for the provided row index. If no error exists returns {@code null}.
     */
    public List<ErrorInfo> getErrorsFor(long index) {
        return errors.get(index);
    }

    /**
     * Adds an error for the index
     */
    public void addErrors(long index, ErrorInfo errorInfo) {
        errors.computeIfAbsent(index, x -> new ArrayList<>()).add(errorInfo);
    }

    /**
     * Returns {@code true} if no row insertion failed, {@code false} otherwise. If {@code false}
     * {@link #getErrors()} ()} returns an empty map.
     */
    public boolean hasErrors() {
        return !errors.isEmpty();
    }

}
