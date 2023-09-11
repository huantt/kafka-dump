package kafka_utils

import "github.com/confluentinc/confluent-kafka-go/kafka"

const DefaultQueueBufferingMaxMessages = 10000

func NewProducer(cfg Config) (*kafka.Producer, error) {
	queueBufferingMaxMessages := DefaultQueueBufferingMaxMessages
	if cfg.QueueBufferingMaxMessages > 0 {
		queueBufferingMaxMessages = cfg.QueueBufferingMaxMessages
	}
	config := kafka.ConfigMap{
		"bootstrap.servers":            cfg.BootstrapServers,
		"queue.buffering.max.messages": queueBufferingMaxMessages, // librdkafka's default value,
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

	if cfg.SSLCALocation != "" && cfg.SSLKeyLocation != "" && cfg.SSLCertLocation != "" && cfg.SSLKeyPassword != "" {
		err := config.SetKey("ssl.ca.location", cfg.SSLCALocation)
		if err != nil {
			return nil, err
		}
		err = config.SetKey("ssl.key.location", cfg.SSLKeyLocation)
		if err != nil {
			return nil, err
		}
		err = config.SetKey("ssl.certificate.location", cfg.SSLCertLocation)
		if err != nil {
			return nil, err
		}
		err = config.SetKey("ssl.key.password", cfg.SSLKeyPassword)
		if err != nil {
			return nil, err
		}
	}

	kafkaProducer, err := kafka.NewProducer(&config)
	if err != nil {
		return nil, err
	}
	return kafkaProducer, nil
}
