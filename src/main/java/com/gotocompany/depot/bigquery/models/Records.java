package com.gotocompany.depot.bigquery.models;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Records {
    private final List<Record> validRecords;
    private final List<Record> invalidRecords;
}
