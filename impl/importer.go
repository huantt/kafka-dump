package impl

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pkg/errors"
)

type Importer struct {
	producer     *kafka.Producer
	reader       Reader
	deliveryChan chan kafka.Event
}

func NewImporter(producer *kafka.Producer, deliveryChan chan kafka.Event, reader Reader) *Importer {
	return &Importer{
		producer:     producer,
		reader:       reader,
		deliveryChan: deliveryChan,
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
		err := i.producer.Produce(&message, i.deliveryChan)
		if err != nil {
			return errors.Wrapf(err, "Failed to produce message: %s", string(message.Value))
		}
	}
	return nil
}
