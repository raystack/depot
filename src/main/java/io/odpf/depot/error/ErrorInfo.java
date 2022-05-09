package io.odpf.depot.error;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@AllArgsConstructor
@Data
@Builder
public class ErrorInfo {
    private Exception exception;
    private ErrorType errorType;

    public String toString() {
        return errorType.name();
    }
}
