package impl

import (
	"encoding/json"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type ParquetWriter struct {
	parquetWriter *writer.ParquetWriter
	fileWriter    source.ParquetFile
}

func NewParquetWriter(fileWriter source.ParquetFile) (*ParquetWriter, error) {
	parquetWriter, err := writer.NewParquetWriter(fileWriter, new(ParquetMessage), 4)
	if err != nil {
		return nil, errors.Wrap(err, "[NewParquetWriter]")
	}
	return &ParquetWriter{
		fileWriter:    fileWriter,
		parquetWriter: parquetWriter,
	}, nil
}

type ParquetMessage struct {
	Value         string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Topic         string `parquet:"name=topic, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Partition     int32  `parquet:"name=partition, type=INT32, convertedtype=INT_32"`
	Offset        string `parquet:"name=offset, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Key           string `parquet:"name=key, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Headers       string `parquet:"name=headers, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	Timestamp     string `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
	TimestampType string `parquet:"name=timestamptype, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN"`
}

func (f *ParquetWriter) Write(msg kafka.Message) (err error) {
	headersBytes, err := json.Marshal(msg.Headers)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal msg.Headers")
	}
	message := ParquetMessage{
		Value:         string(msg.Value),
		Topic:         *msg.TopicPartition.Topic,
		Partition:     msg.TopicPartition.Partition,
		Offset:        msg.TopicPartition.Offset.String(),
		Key:           string(msg.Key),
		Headers:       string(headersBytes),
		Timestamp:     msg.Timestamp.Format(time.RFC3339),
		TimestampType: msg.TimestampType.String(),
	}

	err = f.parquetWriter.Write(message)
	if err != nil {
		return errors.Wrap(err, "[parquetWriter.Write]")
	}
	return err
}

func (f *ParquetWriter) Flush() error {
	err := f.parquetWriter.WriteStop()
	if err != nil {
		return errors.Wrap(err, "[parquetWriter.WriteStop()]")
	}
	err = f.fileWriter.Close()
	if err != nil {
		return errors.Wrap(err, "[fileWriter.Close()]")
	}
	log.Info("Flushed data to file")
	return err
}
