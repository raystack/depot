# Metrics

Depot library has built-in instrumentation with statsd support.
Sinks can have their own metrics, and they will be emmited while using sink connector library into other applications.

## Table of Contents

* [Bigquery Sink](metrics.md#bigquery-sink)
* [Bigtable Sink](metrics.md#bigtable-sink)

## Bigquery Sink

### `Bigquery Operation Total`

Total number of bigquery API operation performed

### `Bigquery Operation Latency`

Time taken for bigquery API operation performed

### `Bigquery Errors Total`

Total numbers of error occurred on bigquery insert operation

## Bigtable Sink

### `Bigtable Operation Total`

Total number of bigtable insert/update operation performed

### `Bigtable Operation Latency`

Time taken for bigtable insert/update operation performed

### `Bigtable Errors Total`

Total numbers of error occurred on bigtable insert/update operation


