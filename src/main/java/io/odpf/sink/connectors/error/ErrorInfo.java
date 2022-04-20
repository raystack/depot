package io.odpf.sink.connectors.error;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class ErrorInfo {
    private Exception exception;
    private ErrorType errorType;

    public String toString() {
        return errorType.name();
    }
}
