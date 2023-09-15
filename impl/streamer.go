package impl

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
)

type Streamer struct {
	consumer     *kafka.Consumer
	producer     *kafka.Producer
	topicFrom    string
	topicTo      string
	deliveryChan chan kafka.Event
	options      StreamerOptions
}

func NewStreamer(consumer *kafka.Consumer, producer *kafka.Producer, topicFrom string, topicTo string, deliveryChan chan kafka.Event, options StreamerOptions) *Streamer {
	return &Streamer{
		consumer:     consumer,
		producer:     producer,
		topicFrom:    topicFrom,
		topicTo:      topicTo,
		deliveryChan: deliveryChan,
		options:      options,
	}
}

type StreamerOptions struct {
	MaxWaitingTimeForNewMessage *time.Duration
}

func (s *Streamer) Run() (transferredCount int64, err error) {
	err = s.consumer.Subscribe(s.topicFrom, nil)
	if err != nil {
		return transferredCount, errors.Wrapf(err, "Failed to subscribe to topic %s", s.topicFrom)
	}
	log.Infof("Subscribed topics: %s", s.topicFrom)
	{
		cx := make(chan os.Signal, 1)
		signal.Notify(cx, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-cx
			err = s.onShutdown()
			if err != nil {
				panic(err)
			}
			os.Exit(1)
		}()
		defer func() {
			err = s.onShutdown()
			if err != nil {
				panic(err)
			}
		}()
	}

	maxWaitingTimeForNewMessage := defaultMaxWaitingTimeForNewMessage
	if s.options.MaxWaitingTimeForNewMessage != nil {
		maxWaitingTimeForNewMessage = *s.options.MaxWaitingTimeForNewMessage
	}
	for {
		msg, err := s.consumer.ReadMessage(maxWaitingTimeForNewMessage)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				log.Infof("Waited for %s but no messages any more! Finish!", maxWaitingTimeForNewMessage)
			}
			return transferredCount, err
		}

		msg.TopicPartition = kafka.TopicPartition{
			Topic:     &s.topicTo,
			Partition: kafka.PartitionAny,
		}
		err = s.producer.Produce(msg, s.deliveryChan)
		if err != nil {
			return transferredCount, errors.Wrapf(err, "Failed to produce message: %s", string(msg.Value))
		}
		_, err = s.consumer.Commit()
		if err != nil {
			return transferredCount, errors.Wrapf(err, "Failed to commit message: %s", string(msg.Value))
		}
		transferredCount++
		log.Infof("Transferred %d messages", transferredCount)
	}
}

func (s *Streamer) onShutdown() error {
	log.Infof("Flushing local queued messages to kafka...")
	s.producer.Flush(30 * 1000)
	return nil
}
