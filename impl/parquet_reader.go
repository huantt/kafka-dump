package impl

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

type ParquetReader struct {
	parquetReader *reader.ParquetReader
	fileReader    source.ParquetFile
}

func NewParquetReader(filePath string) (*ParquetReader, error) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init file reader")
	}

	parquetReader, err := reader.NewParquetReader(fr, new(ParquetMessage), 4)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init parquet reader")
	}
	return &ParquetReader{
		fileReader:    fr,
		parquetReader: parquetReader,
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
				message, err := toKafkaMessage(parquetMessage)
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

func toKafkaMessage(message ParquetMessage) (*kafka.Message, error) {
	var headers []kafka.Header
	if len(message.Headers) > 0 {
		err := json.Unmarshal([]byte(message.Headers), &headers)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to unmarshal kafka headers: %s", message.Headers)
		}
	}
	return &kafka.Message{
		Value: []byte(message.Value),
		TopicPartition: kafka.TopicPartition{
			Topic: &message.Topic,
		},
		Key:     []byte(message.Key),
		Headers: headers,
	}, nil
}
