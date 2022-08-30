package io.odpf.depot.bigtable.parser;

import io.odpf.depot.message.OdpfMessage;

import java.util.Random;

public class BigTableRowKeyParser {
    public String parse(String rowKeyTemplate, OdpfMessage message) {
        Random rand = new Random();
        int n = rand.nextInt();
        System.out.println("Rowkey created: key-test-" + n);
        return "key-test-" + n;
    }
}
