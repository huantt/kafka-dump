package impl

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"syscall"
)

type Importer struct {
	producer        *kafka.Producer
	reader          Reader
	deliveryChan    chan kafka.Event
	overriddenTopic string
}

func NewImporter(producer *kafka.Producer, deliveryChan chan kafka.Event, reader Reader, overriddenTopic string) *Importer {
	return &Importer{
		producer:        producer,
		reader:          reader,
		deliveryChan:    deliveryChan,
		overriddenTopic: overriddenTopic,
	}
}

type Reader interface {
	Read() chan kafka.Message
}

func (i *Importer) Run() error {
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
	messageChn := i.reader.Read()

	for message := range messageChn {
		if i.overriddenTopic != "" {
			message.TopicPartition.Topic = &i.overriddenTopic
		}
		err := i.producer.Produce(&message, i.deliveryChan)
		if err != nil {
			return errors.Wrapf(err, "Failed to produce message: %s", string(message.Value))
		}
	}
	return nil
}
