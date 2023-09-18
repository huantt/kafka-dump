package impl

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
)

type Importer struct {
	logger       log.Logger
	producer     *kafka.Producer
	reader       Reader
	deliveryChan chan kafka.Event
}

func NewImporter(log log.Logger, producer *kafka.Producer, deliveryChan chan kafka.Event, reader Reader) *Importer {
	return &Importer{
		logger:       log,
		producer:     producer,
		reader:       reader,
		deliveryChan: deliveryChan,
	}
}

type Reader interface {
	ReadMessage() chan kafka.Message
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
	messageChn := i.reader.ReadMessage()

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
		}
	}
}
