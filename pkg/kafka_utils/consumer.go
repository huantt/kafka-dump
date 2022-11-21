package kafka_utils

import "github.com/confluentinc/confluent-kafka-go/kafka"

func NewConsumer(cfg Config) (*kafka.Consumer, error) {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  cfg.BootstrapServers,
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
		"group.id":           cfg.GroupId,
	}
	if cfg.SecurityProtocol != "" && cfg.SASLMechanism != "" && cfg.SASLUsername != "" && cfg.SASLPassword != "" {
		err := config.SetKey("security.protocol", cfg.SecurityProtocol)
		if err != nil {
			return nil, err
		}
		err = config.SetKey("sasl.mechanism", cfg.SASLMechanism)
		if err != nil {
			return nil, err
		}
		err = config.SetKey("sasl.username", cfg.SASLUsername)
		if err != nil {
			return nil, err
		}
		err = config.SetKey("sasl.password", cfg.SASLPassword)
		if err != nil {
			return nil, err
		}
	}
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
