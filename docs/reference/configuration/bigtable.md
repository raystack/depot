# Bigtable Sink

A Bigtable sink requires the following variables to be set along with Generic ones

## `SINK_BIGTABLE_GOOGLE_CLOUD_PROJECT_ID`

Contains information of google cloud project id of the bigtable table where the records need to be inserted/updated. Further
documentation on google cloud [project id](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

* Example value: `gcp-project-id`
* Type: `required`

## `SINK_BIGTABLE_INSTANCE_ID`

A Bigtable instance is a container for your data, which contain clusters that your applications can connect to. Each cluster contains nodes, compute units that manage your data and perform maintenance tasks.

A table belongs to an instance, not to a cluster or node. Here you provide the name of that bigtable instance your table belongs to. Further
documentation on [bigtable Instances, clusters, and nodes](https://cloud.google.com/bigtable/docs/instances-clusters-nodes).

* Example value: `cloud-bigtable-instance-id`
* Type: `required`

## `SINK_BIGTABLE_CREDENTIAL_PATH`

Full path of google cloud credentials file. Further documentation of google cloud authentication
and [credentials](https://cloud.google.com/docs/authentication/getting-started).

* Example value: `/.secret/google-cloud-credentials.json`
* Type: `required`

## `SINK_BIGTABLE_TABLE_ID`

Bigtable stores data in massively scalable tables, each of which is a sorted key/value map.

Here you provide the name of the table where the records need to be inserted/updated. Further documentation on
[bigtable tables](https://cloud.google.com/bigtable/docs/managing-tables).

* Example value: `depot-sample-table`
* Type: `required`

## `SINK_BIGTABLE_ROW_KEY_TEMPLATE`

Bigtable tables are composed of rows, each of which typically describes a single entity. Each row is indexed by a single row key.

Here you provide a string template which will be used to create row keys using one or many fields of your input data. Further documentation on [Bigtable storage model](https://cloud.google.com/bigtable/docs/overview#storage-model).

In the example below, If field_1 and field_2 are `String` and `Integer` data types respectively with values as `alpha` and `10` for a specific record, row key generated for this record will be: `key-alpha-10`

* Example value: `key-%s-%d, field_1, field_2`
* Type: `required`

## `SINK_BIGTABLE_COLUMN_FAMILY_MAPPING`

Bigtable columns that are related to one another are typically grouped into a column family. Each column is identified by a combination of the column family and a column qualifier, which is a unique name within the column family.

Here you provide the mapping of the table's `column families` and `qualifiers`, and the field names from input data that we intent to insert into the table. Further documentation on [Bigtable storage model](https://cloud.google.com/bigtable/docs/overview#storage-model).

Please note that `Column families` being provided in this configuration, need to exist in the table beforehand. While `Column Qualifiers` will be created if they don't exist.

* Example value: `{ "depot-sample-family" : { "depot-sample-qualifier-1" : "field_1", "depot-sample-qualifier-2" : "field_7", "depot-sample-qualifier-3" : "field_5"} }`
* Type: `required`
