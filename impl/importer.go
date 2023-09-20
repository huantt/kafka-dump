package impl

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
)

type Importer struct {
	logger        log.Logger
	producer      *kafka.Producer
	reader        Reader
	deliveryChan  chan kafka.Event
	restoreBefore time.Time
	restoreAfter  time.Time
}

func NewImporter(log log.Logger, producer *kafka.Producer, deliveryChan chan kafka.Event, reader Reader, restoreBefore, restoreAfter string) (*Importer, error) {
	var restoreAfterTimeUTC, restoreBeforeTimeUTC time.Time

	if restoreAfter != "" {
		restoreAfterTime, err := time.Parse(time.RFC3339, restoreAfter)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to init importer")
		}
		restoreAfterTimeUTC = restoreAfterTime.UTC()
	}

	if restoreBefore != "" {
		restoreBeforeTime, err := time.Parse(time.RFC3339, restoreBefore)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to init importer")
		}
		restoreBeforeTimeUTC = restoreBeforeTime.UTC()
	}
	return &Importer{
		logger:        log,
		producer:      producer,
		reader:        reader,
		deliveryChan:  deliveryChan,
		restoreBefore: restoreBeforeTimeUTC,
		restoreAfter:  restoreAfterTimeUTC,
	}, nil
}

type Reader interface {
	ReadMessage(restoreBefore, restoreAfter time.Time) chan kafka.Message
	ReadOffset() chan kafka.ConsumerGroupTopicPartitions
}

func (i *Importer) Run(cfg kafka_utils.Config) error {
	cx := make(chan os.Signal, 1)
	signal.Notify(cx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-cx
		i.producer.Flush(30 * 1000)
		os.Exit(1)
	}()
	defer func() {
		i.producer.Flush(30 * 1000)
	}()
	offsetChan := i.reader.ReadOffset()
	messageChn := i.reader.ReadMessage(i.restoreBefore, i.restoreAfter)

	for {
		select {
		case message, ok := <-messageChn:
			if !ok {
				break
			}
			err := i.producer.Produce(&message, i.deliveryChan)
			if err != nil {
				return errors.Wrapf(err, "Failed to produce message: %s", string(message.Value))
			}

		case offsetMessage, ok := <-offsetChan:
			if !ok {
				continue
			}
			cfg.GroupId = offsetMessage.Group
			consumer, err := kafka_utils.NewConsumer(cfg)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init consumer"))
			}
			res, err := consumer.CommitOffsets(offsetMessage.Partitions)
			if err != nil {
				panic(errors.Wrap(err, "Unable to restore offsets of consumer"))
			}
			i.logger.Infof("final result of commit offsets is: %v", res)
			consumer.Close()
		}
	}
}
