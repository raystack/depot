# Development Guide

The following guide will help you quickly run an application which uses depot in your local machine.
The main components of depot sink-connector are:

- Sink: Package which handles sinking data.
- Metrics: Handles the metrics via StatsD client

## Requirements

### Development environment

Java SE Development Kit 8 is required to build, test.
Oracle JDK 8 can be downloaded from [here](https://www.oracle.com/tr/java/technologies/javase/javase8-archive-downloads.html). Extract the tarball to your preferred installation directory and configure your `PATH` environment variable to point to the `bin` sub-directory in the JDK 8 installation directory. For example -

```bash
export PATH=~/Downloads/jdk1.8.0_291/bin:$PATH
```

### Environment Variables

Environment variables can be configured in the following way -

- run `export SAMPLE_VARIABLE=287` on a UNIX shell, to directly assign the required environment variable.

### Custom application

We need to create an application which has org.raystack.depot as a dependency.
This application will create any sink that a developer wants to test by using sink-factories.
The OdpfSink's APIs can be used to send data to sinks and check the response.
One can setup monitoring to see metrics emitted too.

#### Maven and gradle dependency

```xml

<dependency>
    <groupId>org.raystack</groupId>
    <artifactId>depot</artifactId>
    <version>version</version>
</dependency>
```

```sh
implementation group: 'org.raystack', name: 'depot', version: 'version'
```

#### Sample Application

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

### Destination Sink Server

The sink to which the application will send data to, must have its corresponding server set up and configured.
The URL and port address of the database server / HTTP/GRPC endpoint , along with other sink - specific parameters
must be configured the environment variables corresponding to that particular sink.

Configuration parameter variables of each sink can be found in the [Configurations](../reference/configuration/) section.

### Schema Registry

Stencil Server is used as a Schema Registry for hosting Protobuf descriptors.
The environment variable `SCHEMA_REGISTRY_STENCIL_ENABLE` must be set to `true` .
Stencil server URL must be specified in the variable `SCHEMA_REGISTRY_STENCIL_URLS` .
The Proto Descriptor Set file of the messages must be uploaded to the Stencil server.

Refer [this guide](https://github.com/raystack/stencil/tree/master/server#readme) on how to set up and configure the Stencil server, and how to generate and upload Proto descriptor set file to the server.

### Monitoring

Depot supports sinks specific metrics, and it also provides way to send metrics to statsd reporter.
You can set up any visualization platform like grafana for monitoring your custom application.
Following are the typical requirements:

- StatsD host \(e.g. Telegraf\) for aggregation of metrics from StatsD client
- A time-series database \(e.g. InfluxDB\) to store the metrics
- GUI visualization dashboard \(e.g. Grafana\) for detailed visualisation of metrics

### Adding a new Sink

To add a new sink implementation the Sink class has to implement OdpfSink interface

```java
public interface OdpfSink extends Closeable {
    OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException;
}
```

Sink implementations will normally have a factory class too which will be used to setup and create objects of Impl classes.

## Style Guide

### Java

We conform to the [Google Java Style Guide](https://google.github.io/styleguide/javaguide.html). Maven can helpfully take care of that for you before you commit:

## Making a pull request

#### Incorporating upstream changes from master

Our preference is the use of git rebase instead of git merge. Signing commits

```bash
# Include -s flag to signoff
$ git commit -s -m "feat: my first commit"
```

#### Good practices to keep in mind

- Follow the [conventional commit](https://www.conventionalcommits.org/en/v1.0.0/) format for all commit messages.
- Fill in the description based on the default template configured when you first open the PR
- Include kind label when opening the PR
- Avoid force-pushing as it makes reviewing difficult
