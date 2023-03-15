# Depot

Depot contains various common sink implementations and publishes them as a library. This library will be used in
firehose, daggers or any other application which wants to send data to destinations such as service endpoints (HTTP or
GRPC)
& managed databases (Postgres, BigQuery, InfluxDB, Redis, Elasticsearch, Prometheus, MongoDB etc.)

## Key Features

* Instrumentation support with statsd
* Log Sink
* Bigquery Sink
* Redis Sink
* Bigtable Sink

Depot is a sink connector, which acts as a bridge between data processing systems and real sink. The APIs in this
library can be used to push data to various sinks. Common sinks implementations will be added in this repo.

## Requirements

* java8 or higher
* gradle

## How to use

Explore the following resources to get started

* [Reference](docs/reference) contains details about configurations of metrics and various sinks
* [Contribute](docs/contribute/contribution.md) contains resources for anyone who wants to contribute.

### Build and run tests

```sh
# Building the jar
$ ./gradlew clean build

# Running unit tests
$ ./gradlew test

# Run code quality checks
$ ./gradlew checkstyleMain checkstyleTest

#Cleaning the build
$ ./gradlew clean
```

### Maven and gradle dependency

```xml

<dependency>
    <groupId>com.gotocompany</groupId>
    <artifactId>depot</artifactId>
    <version>version</version>
</dependency>
```

```sh
implementation group: 'com.gotocompany', name: 'depot', version: 'version'
```

### Usage example:

```java
public interface Sink extends Closeable {
    SinkResponse pushToSink(List<Message> messages) throws SinkException;
}
```

Sink implementations will normally have a factory class. The application using this library can create and use sinks by
using pseudocode snippet below. For example BigquerySink.

```java
class MyClass {
    void createSink() {
        factory = new BigQuerySinkFactory();
        factory.init();
        sink = factory.create();
    }

    void sendMessages() {
        response = sink.pushToSink(listOfMessage);
        if (response.hasErrors()) {
            // handle errors.
        }
    }
}
```

### Data types

Currently, sink connector library is supporting protobuf and Json format. We can set the datatype of `Message` by
setting `SINK_CONNECTOR_SCHEMA_DATA_TYPE`. Each datatype has parsers which takes care of deserialization.

### Adding a new Sink

Each sink will have to implement `Sink` interface. The pushToSink take a batch of messages and return a response
with error list.

### Configurations

Please check the docs folder for details.
