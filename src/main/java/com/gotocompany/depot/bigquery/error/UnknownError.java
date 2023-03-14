package com.gotocompany.depot.bigquery.error;

import lombok.AllArgsConstructor;

@AllArgsConstructor
/**
 * UnknownError is used when error factory failed to match any possible
 * known errors
 * */
public class UnknownError implements ErrorDescriptor {

    private String reason;

    private String message;

    @Override
    public boolean matches() {
        return false;
    }

    @Override
    public String toString() {
        return String.format("%s: %s", !reason.equals("") ? reason : "UnknownError", message != null ? message : "");
    }
}
