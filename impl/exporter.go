package impl

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
)

type Exporter struct {
	consumer *kafka.Consumer
	topics   []string
	writer   Writer
	options  *Options
}

func NewExporter(consumer *kafka.Consumer, topics []string, writer Writer, options *Options) (*Exporter, error) {
	return &Exporter{
		consumer: consumer,
		topics:   topics,
		writer:   writer,
		options:  options,
	}, nil
}

type Options struct {
	Limit                       uint64
	MaxWaitingTimeForNewMessage *time.Duration
}

type Writer interface {
	Write(msg kafka.Message) error
	Flush() error
}

const defaultMaxWaitingTimeForNewMessage = time.Duration(30) * time.Second

func (e *Exporter) Run() (exportedCount uint64, err error) {
	err = e.consumer.SubscribeTopics(e.topics, nil)
	if err != nil {
		return
	}
	log.Infof("Subscribed topics: %s", e.topics)
	cx := make(chan os.Signal, 1)
	signal.Notify(cx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-cx
		err = e.flushData()
		if err != nil {
			panic(err)
		}
		os.Exit(1)
	}()
	defer func() {
		err = e.flushData()
		if err != nil {
			panic(err)
		}
	}()
	maxWaitingTimeForNewMessage := defaultMaxWaitingTimeForNewMessage
	if e.options.MaxWaitingTimeForNewMessage != nil {
		maxWaitingTimeForNewMessage = *e.options.MaxWaitingTimeForNewMessage
	}
	for {
		msg, err := e.consumer.ReadMessage(maxWaitingTimeForNewMessage)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				log.Infof("Waited for %s but no messages any more! Finish!", maxWaitingTimeForNewMessage)
			}
			return exportedCount, err
		}
		err = e.writer.Write(*msg)
		if err != nil {
			return exportedCount, err
		}
		exportedCount++
		log.Infof("Exported message: %v (Total: %d)", msg.TopicPartition, exportedCount)
		if e.options != nil && exportedCount == e.options.Limit {
			log.Infof("Reached limit %d - Finish!", e.options.Limit)
			return exportedCount, err
		}
	}
}

func (e *Exporter) flushData() error {
	err := e.writer.Flush()
	if err != nil {
		return errors.Wrap(err, "Failed to flush writer")
	}
	_, err = e.consumer.Commit()
	if err != nil {
		if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrNoOffset {
			log.Warnf("No offset, it can happen when there is no message to read, error is: %v", err)
		} else {
			return errors.Wrap(err, "Failed to commit messages")
		}
	}
	return nil
}
