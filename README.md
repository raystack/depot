# Depot

Depot contains various common sink implementations and publishes them as a library. 
This library will be used in firehose, daggers or any other application which wants 
to send data to sinks.

## Key Features
* Log Sink
* Bigquery Sink

Depot is a sink connector, which acts as a bridge between data processing systems 
and real sink. The APIs in this library can be used to push data to various sinks.
Common sinks implementations will be added in this repo.

## Requirements
* java8 or higher
* gradle 

## How to use
### Maven and gradle dependency 
<pre>
&lt;dependency&gt;&#10;  &lt;groupId&gt;io.odpf&lt;/groupId&gt;&#10;  &lt;artifactId&gt;depot&lt;/artifactId&gt;&#10;  &lt;version&gt;0.1.3&lt;/version&gt;&#10;&lt;/dependency&gt;
</pre>
<pre>
implementation group: 'io.odpf', name: 'depot', version: '0.1.3'
</pre>

### Usage example:
<pre>
public interface OdpfSink extends Closeable {
    OdpfSinkResponse pushToSink(List<OdpfMessage> messages) throws OdpfSinkException;
}</pre>
Sink implementations will normally have a factory class. The application using this 
library can create and use sinks by using pseudocode snippet below. For example BigquerySink.
<pre>
factory = new BigQuerySinkFactory(..);
factory.init();
sink = factory.create();
..
response = sink.pushToSink(listOfMessage);
if (response.hasErrors())
// handle errors.
</pre>

### Configurations
Please check the docs folder for details.