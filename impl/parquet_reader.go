package impl

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type ParquetReader struct {
	parquetReader             *reader.ParquetReader
	fileReader                source.ParquetFile
	includePartitionAndOffset bool
}

func NewParquetReader(filePath string, includePartitionAndOffset bool) (*ParquetReader, error) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init file reader")
	}

	parquetReader, err := reader.NewParquetReader(fr, new(ParquetMessage), 4)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init parquet reader")
	}
	return &ParquetReader{
		fileReader:                fr,
		parquetReader:             parquetReader,
		includePartitionAndOffset: includePartitionAndOffset,
	}, nil
}

const batchSize = 10

func (p *ParquetReader) Read() chan kafka.Message {
	rowNum := int(p.parquetReader.GetNumRows())
	ch := make(chan kafka.Message, batchSize)
	counter := 0
	go func() {
		for i := 0; i < rowNum/batchSize+1; i++ {
			parquetMessages := make([]ParquetMessage, batchSize)
			if err := p.parquetReader.Read(&parquetMessages); err != nil {
				err = errors.Wrap(err, "Failed to bulk read messages from parquet file")
				panic(err)
			}

			for _, parquetMessage := range parquetMessages {
				counter++
				message, err := toKafkaMessage(parquetMessage, p.includePartitionAndOffset)
				if err != nil {
					err = errors.Wrapf(err, "Failed to parse kafka message from parquet message")
					panic(err)
				}
				ch <- *message
				log.Infof("Loaded %f% (%d/%d)", counter/rowNum, counter, rowNum)
			}
		}
		p.parquetReader.ReadStop()
		err := p.fileReader.Close()
		if err != nil {
			panic(errors.Wrap(err, "Failed to close fileReader"))
		}
	}()
	return ch
}

func (p *ParquetReader) GetNumberOfRows() int64 {
	return p.parquetReader.GetNumRows()
}

func toKafkaMessage(message ParquetMessage, includePartitionAndOffset bool) (*kafka.Message, error) {
	timestamp, err := time.Parse(time.RFC3339, message.Timestamp)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to convert string to time.Time: %s", message.Timestamp)
	}

	var headers []kafka.Header
	if len(message.Headers) > 0 {
		err := json.Unmarshal([]byte(message.Headers), &headers)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to unmarshal kafka headers: %s", message.Headers)
		}
	}

	var timestampType int
	switch message.TimestampType {
	case kafka.TimestampCreateTime.String():
		timestampType = int(kafka.TimestampCreateTime)
	case kafka.TimestampLogAppendTime.String():
		timestampType = int(kafka.TimestampLogAppendTime)
	case kafka.TimestampNotAvailable.String():
		fallthrough
	default:
		timestampType = int(kafka.TimestampNotAvailable)
	}

	kafkaMessage := &kafka.Message{
		Value: []byte(message.Value),
		TopicPartition: kafka.TopicPartition{
			Topic: &message.Topic,
		},
		Key:           []byte(message.Key),
		Headers:       headers,
		Timestamp:     timestamp,
		TimestampType: kafka.TimestampType(timestampType),
	}

	if includePartitionAndOffset {
		offset, err := strconv.Atoi(message.Offset)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to convert string to int for message offset: %s", message.Offset)
		}
		kafkaMessage.TopicPartition.Offset = kafka.Offset(offset)
		kafkaMessage.TopicPartition.Partition = message.Partition
	}

	return kafkaMessage, nil
}
