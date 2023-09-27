package impl

import (
	"encoding/json"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetWriter struct {
	parquetWriterMessage *writer.ParquetWriter
	parquetWriterOffset  *writer.ParquetWriter
	fileWriterMessage    source.ParquetFile
	fileWriterOffset     source.ParquetFile
}

func NewParquetWriter(fileWriterMessage, fileWriterOffset source.ParquetFile) (*ParquetWriter, error) {
	parquetWriterMessage, err := writer.NewParquetWriter(fileWriterMessage, new(KafkaMessage), 9)
	if err != nil {
		return nil, errors.Wrap(err, "[NewParquetWriter]")
	}

	parquetWriterOffset, err := writer.NewParquetWriter(fileWriterOffset, new(OffsetMessage), 4)
	if err != nil {
		return nil, errors.Wrap(err, "[NewParquetWriter]")
	}

	return &ParquetWriter{
		fileWriterMessage:    fileWriterMessage,
		parquetWriterMessage: parquetWriterMessage,
		parquetWriterOffset:  parquetWriterOffset,
		fileWriterOffset:     fileWriterOffset,
	}, nil
}

type KafkaMessage struct {
	Value         string  `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Topic         string  `parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Partition     int32   `parquet:"name=partition, type=INT32, convertedtype=INT_32"`
	Offset        string  `parquet:"name=offset, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Key           string  `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Headers       string  `parquet:"name=headers, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Timestamp     string  `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	TimestampType string  `parquet:"name=timestamptype, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Metadata      *string `parquet:"name=metadata, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

type OffsetMessage struct {
	GroupID             string `parquet:"name=groupid, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Topic               string `parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Partition           int32  `parquet:"name=partition, type=INT32, convertedtype=INT_32"`
	Offset              string `parquet:"name=offset, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	WatermarkOffsetLow  int64  `parquet:"name=watermarklow, type=INT64, convertedtype=INT_64"`
	WatermarkOffsetHigh int64  `parquet:"name=watermakehigh, type=INT64, convertedtype=INT_64"`
}

type kafkaTopicPartition struct {
	kafka.TopicPartition
	WOH int64
	WOL int64
}

type kafkaOffsetMessage struct {
	Group                string
	KafkaTopicPartitions []kafkaTopicPartition
}

func (f *ParquetWriter) Write(msg kafka.Message) (err error) {
	headersBytes, err := json.Marshal(msg.Headers)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal msg.Headers")
	}
	message := KafkaMessage{
		Value:         string(msg.Value),
		Topic:         *msg.TopicPartition.Topic,
		Partition:     msg.TopicPartition.Partition,
		Offset:        msg.TopicPartition.Offset.String(),
		Metadata:      msg.TopicPartition.Metadata,
		Key:           string(msg.Key),
		Headers:       string(headersBytes),
		Timestamp:     msg.Timestamp.Format(time.RFC3339),
		TimestampType: msg.TimestampType.String(),
	}

	err = f.parquetWriterMessage.Write(message)
	if err != nil {
		return errors.Wrap(err, "[parquetWriter.Write]")
	}
	return err
}

func (f *ParquetWriter) OffsetWrite(msg kafkaOffsetMessage) (err error) {
	for _, partition := range msg.KafkaTopicPartitions {
		message := OffsetMessage{
			GroupID:             msg.Group,
			Topic:               *partition.Topic,
			Partition:           partition.Partition,
			Offset:              partition.Offset.String(),
			WatermarkOffsetLow:  partition.WOL,
			WatermarkOffsetHigh: partition.WOH,
		}

		err = f.parquetWriterOffset.Write(message)
		if err != nil {
			return errors.Wrap(err, "[parquetWriter.Write]")
		}
	}

	return err
}

func (f *ParquetWriter) Flush() error {
	err := f.parquetWriterMessage.WriteStop()
	if err != nil {
		return errors.Wrap(err, "[parquetWriterMessage.WriteStop()]")
	}
	err = f.parquetWriterOffset.WriteStop()
	if err != nil {
		return errors.Wrap(err, "[parquetWriterOffset.WriteStop()]")
	}
	err = f.fileWriterMessage.Close()
	if err != nil {
		return errors.Wrap(err, "[fileWriterMessage.Close()]")
	}
	err = f.fileWriterOffset.Close()
	if err != nil {
		return errors.Wrap(err, "[fileWriterOffset.Close()]")
	}
	log.Info("Flushed data to file")
	return err
}
