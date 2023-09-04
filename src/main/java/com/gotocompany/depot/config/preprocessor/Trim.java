package com.gotocompany.depot.config.preprocessor;

import org.aeonbits.owner.Preprocessor;

public class Trim implements Preprocessor {
    @Override
    public String process(String input) {
        return input == null ? null : input.trim();
    }
}
