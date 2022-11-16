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
```shell
OUT_PATH=/path/to/output/data.parquet \
KAFKA_GROUP_ID=kafka-consumer-group-id \
KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
KAFKA_SECURITY_PROTOCOL=SASL_SSL \
KAFKA_SASL_MECHANISM=PLAIN \
KAFKA_SASL_USERNAME=admin \
KAFKA_SASL_PASSWORD=admin \
kafka-dump export
```

### Import Kafka topics from parquet file
```shell
INPUT_PATH=out/path/to/input/data.parquet \
KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
KAFKA_SECURITY_PROTOCOL=SASL_SSL \
KAFKA_SASL_MECHANISM=PLAIN \
KAFKA_SASL_USERNAME=admin \
KAFKA_SASL_PASSWORD=admin \
kafka-dump import
```

# Use Docker
### Export Kafka topics to parquet file
```shell
docker run -d --rm \
OUT_PATH=/path/to/output/data.parquet \
KAFKA_GROUP_ID=kafka-consumer-group-id \
KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
KAFKA_SECURITY_PROTOCOL=SASL_SSL \
KAFKA_SASL_MECHANISM=PLAIN \
KAFKA_SASL_USERNAME=admin \
KAFKA_SASL_PASSWORD=admin \
huanttok/kafka-dump:latest \
kafka-dump export
```
### Import Kafka topics from parquet file
```shell
docker run -d --rm \
-e INPUT_PATH=out/path/to/input/data.parquet \
-e KAFKA_BOOTSTRAP_SERVERS=kafka-host:9092 \
-e KAFKA_SECURITY_PROTOCOL=SASL_SSL \
-e KAFKA_SASL_MECHANISM=PLAIN \
-e KAFKA_SASL_USERNAME=admin \
-e KAFKA_SASL_PASSWORD=admin \
huanttok/kafka-dump:latest \
kafka-dump import
```