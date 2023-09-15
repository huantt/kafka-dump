package kafka_utils

import "github.com/confluentinc/confluent-kafka-go/v2/kafka"

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

	if !cfg.EnableAutoOffsetStore {
		err := config.SetKey("enable.auto.offset.store", cfg.EnableAutoOffsetStore)
		if err != nil {
			return nil, err
		}
	}

	if cfg.QueuedMaxMessagesKbytes > 0 {
		err := config.SetKey("fetch.message.max.bytes", cfg.FetchMessageMaxBytes)
		if err != nil {
			return nil, err
		}
	}
	if cfg.FetchMessageMaxBytes > 0 {
		err := config.SetKey("queued.max.messages.kbytes", cfg.QueuedMaxMessagesKbytes)
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
