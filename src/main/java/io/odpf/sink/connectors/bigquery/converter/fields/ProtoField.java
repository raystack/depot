package io.odpf.sink.connectors.bigquery.converter.fields;

public interface ProtoField {

    Object getValue();

    boolean matches();
}
