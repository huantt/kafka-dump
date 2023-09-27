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
	parquetReaderMessage      *reader.ParquetReader
	parquetReaderOffset       *reader.ParquetReader
	fileReaderMessage         source.ParquetFile
	fileReaderOffset          source.ParquetFile
	includePartitionAndOffset bool
}

func NewParquetReader(filePathMessage, filePathOffset string, includePartitionAndOffset bool) (*ParquetReader, error) {
	var fileReaderOffset source.ParquetFile
	var parquetReaderOffset *reader.ParquetReader
	fileReaderMessage, err := local.NewLocalFileReader(filePathMessage)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init file reader")
	}

	parquetReaderMessage, err := reader.NewParquetReader(fileReaderMessage, new(KafkaMessage), 9)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to init parquet reader")
	}

	if filePathOffset != "" {
		fileReaderOffset, err = local.NewLocalFileReader(filePathOffset)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to init file reader")
		}

		parquetReaderOffset, err = reader.NewParquetReader(fileReaderOffset, new(OffsetMessage), 4)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to init parquet reader")
		}
	}
	return &ParquetReader{
		fileReaderMessage:         fileReaderMessage,
		fileReaderOffset:          fileReaderOffset,
		parquetReaderMessage:      parquetReaderMessage,
		parquetReaderOffset:       parquetReaderOffset,
		includePartitionAndOffset: includePartitionAndOffset,
	}, nil
}

const batchSize = 10

func (p *ParquetReader) ReadMessage(restoreBefore, restoreAfter time.Time, doneChan chan int) chan kafka.Message {
	ch := make(chan kafka.Message, batchSize)
	if p.parquetReaderMessage == nil {
		return ch
	}
	rowNum := int(p.parquetReaderMessage.GetNumRows())
	counter := 0
	go func() {
		for i := 0; i < rowNum/batchSize+1; i++ {
			kafkaMessages := make([]KafkaMessage, batchSize)
			if err := p.parquetReaderMessage.Read(&kafkaMessages); err != nil {
				err = errors.Wrap(err, "Failed to bulk read messages from parquet file")
				panic(err)
			}

			for _, parquetMessage := range kafkaMessages {
				counter++
				message, err := toKafkaMessage(parquetMessage, p.includePartitionAndOffset, restoreBefore, restoreAfter)
				if err != nil {
					err = errors.Wrapf(err, "Failed to parse kafka message from parquet message")
					panic(err)
				}
				if message != nil {
					ch <- *message
					log.Infof("Loaded %f% (%d/%d)", counter/rowNum, counter, rowNum)
				}
			}
		}
		p.parquetReaderMessage.ReadStop()
		err := p.fileReaderMessage.Close()
		if err != nil {
			panic(errors.Wrap(err, "Failed to close fileReader"))
		}
		close(ch)
		doneChan <- 0
		close(doneChan)
		log.Infof("kafka message restored successfully")
	}()
	return ch
}

func (p *ParquetReader) ReadOffset(doneChan chan int) chan kafka.ConsumerGroupTopicPartitions {
	ch := make(chan kafka.ConsumerGroupTopicPartitions, batchSize)
	// When offset file is not given
	if p.parquetReaderOffset == nil {
		return ch
	}
	rowNum := int(p.parquetReaderOffset.GetNumRows())
	counter := 0
	// When offset file is empty
	if rowNum == 0 {
		return ch
	}
	go func(doneChan chan int) {
		// wait for all the messages to be restore first, then
		// restore the kafka consumer group offset
		val := <-doneChan
		if val == 0 {
			for i := 0; i < rowNum/batchSize+1; i++ {
				offsetMessages := make([]OffsetMessage, batchSize)
				if err := p.parquetReaderOffset.Read(&offsetMessages); err != nil {
					err = errors.Wrap(err, "Failed to bulk read messages from parquet file")
					panic(err)
				}

				resMessages, err := toKafkaConsumerGroupTopicPartitions(offsetMessages)
				if err != nil {
					err = errors.Wrapf(err, "Failed to parse offset messages from offset file")
					panic(err)
				}

				for _, offsetMessage := range resMessages {
					counter++
					log.Infof("offset message is: %v", offsetMessage)
					ch <- offsetMessage
					log.Infof("Loaded %f% (%d/%d)", counter/rowNum, counter, rowNum)
				}
			}
			p.parquetReaderOffset.ReadStop()
			err := p.fileReaderOffset.Close()
			if err != nil {
				panic(errors.Wrap(err, "Failed to close fileReader"))
			}
			close(ch)
			log.Infof("consumer offset restored successfully")
		}
	}(doneChan)
	return ch
}

func (p *ParquetReader) GetNumberOfRowsInMessageFile() int64 {
	return p.parquetReaderMessage.GetNumRows()
}

func (p *ParquetReader) GetNumberOfRowsInOffsetFile() int64 {
	return p.parquetReaderOffset.GetNumRows()
}

func toKafkaMessage(message KafkaMessage, includePartitionAndOffset bool, restoreBefore, restoreAfter time.Time) (*kafka.Message, error) {
	timestamp, err := time.Parse(time.RFC3339, message.Timestamp)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to convert string to time.Time: %s", message.Timestamp)
	}

	if !restoreBefore.IsZero() {
		if !timestamp.Before(restoreBefore) {
			return nil, nil
		}
	}

	if !restoreAfter.IsZero() {
		if !timestamp.After(restoreAfter) {
			return nil, nil
		}
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
		kafkaOffset := &kafkaMessage.TopicPartition.Offset
		err = kafkaOffset.Set(message.Offset)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to set offset for message offset: %s", message.Offset)
		}
		kafkaMessage.TopicPartition.Partition = message.Partition
	}

	return kafkaMessage, nil
}

func toKafkaConsumerGroupTopicPartitions(offsetMessages []OffsetMessage) ([]kafka.ConsumerGroupTopicPartitions, error) {
	res := make([]kafka.ConsumerGroupTopicPartitions, 0)
	groupIDToPartitions := make(map[string][]kafka.TopicPartition)
	if len(offsetMessages) > 0 {
		for _, offsetMessage := range offsetMessages {
			var topicPartition kafka.TopicPartition
			offset, err := modifyOffset(offsetMessage)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to set offset during consumer offset restore: %s", offsetMessage.Offset)
			}
			topicPartition.Offset = offset
			topicPartition.Partition = offsetMessage.Partition
			topicPartition.Topic = &offsetMessage.Topic
			if val, ok := groupIDToPartitions[offsetMessage.GroupID]; !ok {
				topicPartitions := make(kafka.TopicPartitions, 0)
				topicPartitions = append(topicPartitions, topicPartition)
				groupIDToPartitions[offsetMessage.GroupID] = topicPartitions
			} else {
				val = append(val, topicPartition)
				groupIDToPartitions[offsetMessage.GroupID] = val
			}
		}

		for k, v := range groupIDToPartitions {
			var consumerGroupTopicPartition kafka.ConsumerGroupTopicPartitions
			consumerGroupTopicPartition.Group = k
			consumerGroupTopicPartition.Partitions = v
			res = append(res, consumerGroupTopicPartition)
		}
	} else {
		return res, errors.New("nothing to read!!!")
	}

	return res, nil
}

func modifyOffset(OM OffsetMessage) (kafka.Offset, error) {
	switch OM.Offset {
	case "beginning":
		fallthrough
	case "earliest":
		return kafka.Offset(kafka.OffsetBeginning), nil

	case "end":
		fallthrough
	case "latest":
		return kafka.Offset(kafka.OffsetEnd), nil

	case "unset":
		fallthrough
	case "invalid":
		return kafka.Offset(kafka.OffsetInvalid), nil

	case "stored":
		return kafka.Offset(kafka.OffsetStored), nil

	default:
		off, err := strconv.Atoi(OM.Offset)
		if off == int(OM.WatermarkOffsetHigh) {
			return kafka.Offset(off), err
		}
		return kafka.Offset(off + 1), err
	}
}
