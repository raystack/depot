package io.odpf.sink.connectors.config;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Tuple<T, V> {
    private T first;
    private V second;
}
