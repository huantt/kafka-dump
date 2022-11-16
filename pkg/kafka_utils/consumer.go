package kafka_utils

import "github.com/confluentinc/confluent-kafka-go/kafka"

func NewConsumer(cfg Config) (*kafka.Consumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"security.protocol":  cfg.SecurityProtocol,
		"sasl.mechanism":     cfg.SASLMechanism,
		"sasl.username":      cfg.SASLUsername,
		"sasl.password":      cfg.SASLPassword,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
		"group.id":           cfg.GroupId,
	})
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
