package com.gotocompany.depot.common;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Tuple<T, V> {
    private T first;
    private V second;
}
