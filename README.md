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
kafka-dump export \
--file=path/to/output/data.parquet \
--kafka-topics=users-activities \
--kafka-group-id=id=kafka-dump.local \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

### Count number of rows in parquet file
```shell
kafka-dump count-parquet-rows \
--file=path/to/output/data.parquet
```
### Import Kafka topics from parquet file
```shell
kafka-dump import \
--file=path/to/input/data.parquet \
--kafka-servers=localhost:9092 \
--kafka-username=admin \
--kafka-password=admin \
--kafka-security-protocol=SASL_SSL \
--kafka-sasl-mechanism=PLAIN
```

## Use Docker
```shell
docker run -d --rm \
-v /local-data:/data \
huanttok/kafka-dump:latest \
./kafka-dump export \
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
- Improve exporting speed by reducing number of commit times
- Export topic partitions in parallel