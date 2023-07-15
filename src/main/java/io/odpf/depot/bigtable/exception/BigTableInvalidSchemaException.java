package org.raystack.depot.bigtable.exception;

public class BigTableInvalidSchemaException extends RuntimeException {
    public BigTableInvalidSchemaException(String message, Throwable cause) {
        super(message, cause);
    }

    public BigTableInvalidSchemaException(String messsage) {
        super(messsage);
    }
}
