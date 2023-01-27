# Kafka Connect BigQuery Storage Write Connector

[![CircleCI](https://dl.circleci.com/status-badge/img/gh/joker1007/kafka-connect-bigquery-storage-write/tree/main.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/joker1007/kafka-connect-bigquery-storage-write/tree/main)

This is an implementation of a sink connector from [Apache Kafka](http://kafka.apache.org) to [Google BigQuery](https://cloud.google.com/bigquery/) using [Storage Write API](https://cloud.google.com/bigquery/docs/write-api).

# Configuration

| name        | type                            | required | default   | description                                                                                       |
|-------------|---------------------------------|----------|-----------|---------------------------------------------------------------------------------------------------|
| project     | string                          | true     |           | The BigQuery project name to write to                                                             |
| dataset     | string                          | true     |           | The BigQuery dataset name to write to                                                             |
| table       | string                          | true     |           | The BigQuery table name to write to                                                               |
| keyfile     | string                          | true     |           | The filepath of a JSON key with BigQuery service account credentials                              |
| write.mode  | enum (`commtted` or `pending` ) | false    | committed | This value set stream mode (see [BigQuery Doc](https://cloud.google.com/bigquery/docs/write-api)) |
| buffer.size | int                             | false    | 1000      | The number of records kept in buffer before transport                                             |

## Example

```json
{
  "name": "bigquery-sink",
  "config": {
    "connector.class": "com.reproio.kafka.connect.bigquery.BigqueryStorageWriteSinkConnector",
    "tasks.max" : "4",
    "topics" : "CustomEventLogs",
    "project" : "sample-project",
    "dataset" : "sample_dataset",
    "table": "sample_table",
    "keyfile" : " /opt/host/gcp_key.json"
  }
}
```
