package io.odpf.depot.error;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@AllArgsConstructor
@Data
@Builder
public class ErrorInfo {

    @EqualsAndHashCode.Exclude private Exception exception;
    private ErrorType errorType;

    public String toString() {
        return errorType.name();
    }
}
