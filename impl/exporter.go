package impl

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Exporter struct {
	consumer *kafka.Consumer
	topics   []string
	writer   Writer
}

func NewExporter(consumer *kafka.Consumer, topics []string, writer Writer) (*Exporter, error) {
	return &Exporter{
		consumer: consumer,
		topics:   topics,
		writer:   writer,
	}, nil
}

type Writer interface {
	Write(msg kafka.Message) error
	Flush() error
}

const maxWaitingTimeForNewMessage = time.Duration(10) * time.Second

func (e *Exporter) Run() (err error) {
	err = e.consumer.SubscribeTopics(e.topics, nil)
	if err != nil {
		return err
	}
	log.Infof("Subscribed topics: %s", e.topics)
	cx := make(chan os.Signal, 1)
	signal.Notify(cx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-cx
		err = e.onShutdown()
		if err != nil {
			panic(err)
		}
		os.Exit(1)
	}()
	defer func() {
		err = e.onShutdown()
		if err != nil {
			panic(err)
		}
	}()
	for {
		msg, err := e.consumer.ReadMessage(maxWaitingTimeForNewMessage)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				log.Infof("Waited for %s but no messages any more! Finish!", maxWaitingTimeForNewMessage)
			}
			return err
		}
		log.Debugf("Received message: %s", string(msg.Value))
		err = e.writer.Write(*msg)
		if err != nil {
			return err
		}
		_, err = e.consumer.Commit()
		if err != nil {
			return errors.Wrap(err, "Failed to commit messages")
		}
	}
}

func (e *Exporter) onShutdown() error {
	err := e.writer.Flush()
	if err != nil {
		return errors.Wrap(err, "Failed to flush writer")
	}
	return nil
}
