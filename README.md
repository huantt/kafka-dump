# Kafka data backup
Kafka dump is a tool to back up and restore your Kafka data.

It helps you reduce the cost of storing the data that you don't need to use right now but can not delete.

In other words, this tool is used to back up and restore `Cold data` for Kafka topics.

## Use command line
### Install
```shell
go install github.com/huantt/kafka-dump@latest
```
```shell
export PATH=$PATH:$(go env GOPATH)/bin
```
### Export Kafka topics to parquet file
#### Options
```shell
Usage:
   export [flags]

Flags:
      --concurrent-consumers int                  Number of concurrent consumers (default 1)
      --enable-auto-offset-store                  To store offset in kafka broker (default true)
      --fetch-message-max-bytes int               Maximum number of bytes per topic+partition to request when fetching messages from the broker. (default 1048576)
  -f, --file string                               Output file path (required)
      --gcs-bucket string                         Google Cloud Storage bucket name
      --gcs-project-id string                     Google Cloud Storage Project ID
      --google-credentials string                 Path to Google Credentials file
  -h, --help                                      help for export
      --kafka-group-id string                     Kafka consumer group ID
      --kafka-password string                     Kafka password
      --kafka-sasl-mechanism string               Kafka password
      --kafka-security-protocol string            Kafka security protocol
      --kafka-servers string                      Kafka servers string
      --kafka-topics stringArray                  Kafka topics
      --kafka-username string                     Kafka username
      --limit uint                                Supports file splitting. Files are split by the number of messages specified
      --max-waiting-seconds-for-new-message int   Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever. (default 30)
      --queued-max-messages-kbytes int            Maximum number of kilobytes per topic+partition in the local consumer queue. This value may be overshot by fetch.message.max.bytes (default 128000)
      --ssl-ca-location string                    Location of client ca cert file in pem
      --ssl-certificate-location string           Client certificate location
      --ssl-key-location string                   Path to ssl private key
      --ssl-key-password string                   Password for ssl private key passphrase
      --storage string                            Storage type: local file (file) or Google cloud storage (gcs) (default "file")

Global Flags:
      --log-level string   Log level (default "info")
```
#### Sample

- Connect to Kafka cluster without the SSL encryption being enabled for exporting the data.
```shell
kafka-dump export \
--storage=file
--file=path/to/output/data.parquet \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

- Connect to Kafka cluster with the SSL encryption being enabled for exporting the data.
```shell
kafka-dump export \
--storage=file
--file=path/to/output/data.parquet \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SSL \
--kafka-sasl-mechanism=PLAIN \
--ssl-ca-location=<path to ssl cacert> \
--ssl-certificate-location=<path to ssl cert> \
--ssl-key-location=<path to ssl key> \
--ssl-key-password=<ssl key password>
```

### Import Kafka topics from parquet file
```shell
Usage:
   import [flags]

Flags:
  -f, --file string                       Output file path (required)
  -h, --help                              help for import
  -i, --include-partition-and-offset      To store partition and offset of kafka message in file
      --kafka-password string             Kafka password
      --kafka-sasl-mechanism string       Kafka password
      --kafka-security-protocol string    Kafka security protocol
      --kafka-servers string              Kafka servers string
      --kafka-username string             Kafka username
      --ssl-ca-location string            Location of client ca cert file in pem
      --ssl-certificate-location string   Client certificate location
      --ssl-key-location string           Path to ssl private key
      --ssl-key-password string           Password for ssl private key passphrase

Global Flags:
      --log-level string   Log level (default "info")
```
#### Sample

- Connect to Kafka cluster without the SSL encryption being enabled for importing the data.
```shell
kafka-dump import \
--file=path/to/input/data.parquet \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

- Connect to Kafka cluster with the SSL encryption being enabled for importing the data.
```shell
kafka-dump import \
--file=path/to/input/data.parquet \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SSL \
--kafka-sasl-mechanism=PLAIN \
--ssl-ca-location=<path to ssl cacert> \
--ssl-certificate-location=<path to ssl cert> \
--ssl-key-location=<path to ssl key> \
--ssl-key-password=<ssl key password>
```

- In order to produce data in the internal topics of Kafka cluster (for eg. "__consumer_offsets"), the client id of the producer needs to be configured as `__admin_client`.
```shell
kafka-dump import \
--file=path/to/input/data.parquet \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SSL \
--kafka-sasl-mechanism=PLAIN \
--client-id=__admin_client \
--ssl-ca-location=<path to ssl cacert> \
--ssl-certificate-location=<path to ssl cert> \
--ssl-key-location=<path to ssl key> \
--ssl-key-password=<ssl key password>
```

### Stream messages topic to topic
```shell
Usage:
   stream [flags]

Flags:
      --from-kafka-group-id string                Kafka consumer group ID
      --from-kafka-password string                Source Kafka password
      --from-kafka-sasl-mechanism string          Source Kafka password
      --from-kafka-security-protocol string       Source Kafka security protocol
      --from-kafka-servers string                 Source Kafka servers string
      --from-kafka-username string                Source Kafka username
      --from-topic string                         Source topic
      -h, --help                                      help for stream
      --max-waiting-seconds-for-new-message int   Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever. (default 30)
      --to-kafka-password string                  Destination Kafka password
      --to-kafka-sasl-mechanism string            Destination Kafka password
      --to-kafka-security-protocol string         Destination Kafka security protocol
      --to-kafka-servers string                   Destination Kafka servers string
      --to-kafka-username string                  Destination Kafka username
      --to-topic string                           Destination topic

Global Flags:
      --log-level string   Log level (default "info")

```

#### Sample
```shell
kafka-dump stream \
--from-topic=users \
--from-kafka-group-id=stream \
--from-kafka-servers=localhost:9092 \
--from-kafka-username=admin \
--from-kafka-password=admin \
--from-kafka-security-protocol=SASL_SSL \
--from-kafka-sasl-mechanism=PLAIN \
--to-topic=new-users \
--to-kafka-servers=localhost:9092 \
--to-kafka-username=admin \
--to-kafka-password=admin \
--to-kafka-security-protocol=SASL_SSL \
--to-kafka-sasl-mechanism=PLAIN
--max-waiting-seconds-for-new-message=-1
```

### Count number of rows in parquet file
```shell
Usage:
   count-parquet-rows [flags]

Flags:
  -f, --file string   File path (required)
  -h, --help          help for count-parquet-rows

Global Flags:
      --log-level string   Log level (default "info")
```
#### Sample
```shell
kafka-dump count-parquet-rows \
--file=path/to/output/data.parquet
```

## Use Docker
```shell
docker run -d --rm \
-v /local-data:/data \
huanttok/kafka-dump:latest \
kafka-dump export \
--file=/data/path/to/output/data.parquet \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

## TODO
- Import topics from multiple files or directory
- Import topics from Google Cloud Storage files or directory
