package cmd

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/spf13/cobra"
	"time"
)

func CreateStreamCmd() (*cobra.Command, error) {
	var fromKafkaServers string
	var fromKafkaUsername string
	var fromKafkaPassword string
	var fromKafkaSecurityProtocol string
	var fromKafkaSASKMechanism string
	var fromKafkaGroupID string

	var toKafkaServers string
	var toKafkaUsername string
	var toKafkaPassword string
	var toKafkaSecurityProtocol string
	var toKafkaSASKMechanism string

	var fromTopic string
	var toTopic string

	var maxWaitingSecondsForNewMessage int

	command := cobra.Command{
		Use: "stream",
		Run: func(cmd *cobra.Command, args []string) {
			kafkaConsumerConfig := kafka_utils.Config{
				BootstrapServers: fromKafkaServers,
				SecurityProtocol: fromKafkaSecurityProtocol,
				SASLMechanism:    fromKafkaSASKMechanism,
				SASLUsername:     fromKafkaUsername,
				SASLPassword:     fromKafkaPassword,
				GroupId:          fromKafkaGroupID,
			}
			consumer, err := kafka_utils.NewConsumer(kafkaConsumerConfig)
			if err != nil {
				panic(err)
			}
			kafkaProducerConfig := kafka_utils.Config{
				BootstrapServers: fromKafkaServers,
				SecurityProtocol: fromKafkaSecurityProtocol,
				SASLMechanism:    fromKafkaSASKMechanism,
				SASLUsername:     fromKafkaUsername,
				SASLPassword:     fromKafkaPassword,
			}
			producer, err := kafka_utils.NewProducer(kafkaProducerConfig)
			if err != nil {
				panic(err)
			}
			queueBufferingMaxMessages := kafka_utils.DefaultQueueBufferingMaxMessages
			if kafkaProducerConfig.QueueBufferingMaxMessages > 0 {
				queueBufferingMaxMessages = kafkaProducerConfig.QueueBufferingMaxMessages
			}
			deliveryChan := make(chan kafka.Event, queueBufferingMaxMessages)
			go func() { // Tricky: kafka require specific deliveryChan to use Flush function
				for e := range deliveryChan {
					m := e.(*kafka.Message)
					if m.TopicPartition.Error != nil {
						panic(fmt.Sprintf("Failed to deliver message: %v\n", m.TopicPartition))
					} else {
						log.Debugf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			}()
			maxWaitingSecondsForNewMessage := time.Duration(10) * time.Second
			streamer := impl.NewStreamer(
				consumer,
				producer,
				fromTopic,
				toTopic,
				deliveryChan,
				impl.StreamerOptions{MaxWaitingTimeForNewMessage: &maxWaitingSecondsForNewMessage},
			)
			transferredCount, err := streamer.Run()
			if err != nil {
				panic(err)
			}
			log.Infof("Transferred %d messages from %s to %s", transferredCount, fromTopic, toTopic)
		},
	}
	command.Flags().StringVar(&fromKafkaServers, "from-kafka-servers", "", "Source Kafka servers string")
	command.Flags().StringVar(&fromKafkaUsername, "from-kafka-username", "", "Source Kafka username")
	command.Flags().StringVar(&fromKafkaPassword, "from-kafka-password", "", "Source Kafka password")
	command.Flags().StringVar(&fromKafkaSASKMechanism, "from-kafka-sasl-mechanism", "", "Source Kafka password")
	command.Flags().StringVar(&fromKafkaSecurityProtocol, "from-kafka-security-protocol", "", "Source Kafka security protocol")
	command.Flags().StringVar(&fromKafkaGroupID, "from-kafka-group-id", "", "Kafka consumer group ID")
	command.Flags().StringVar(&fromTopic, "from-topic", "", "Source topic")

	command.Flags().StringVar(&toKafkaServers, "to-kafka-servers", "", "Destination Kafka servers string")
	command.Flags().StringVar(&toKafkaUsername, "to-kafka-username", "", "Destination Kafka username")
	command.Flags().StringVar(&toKafkaPassword, "to-kafka-password", "", "Destination Kafka password")
	command.Flags().StringVar(&toKafkaSASKMechanism, "to-kafka-sasl-mechanism", "", "Destination Kafka password")
	command.Flags().StringVar(&toKafkaSecurityProtocol, "to-kafka-security-protocol", "", "Destination Kafka security protocol")
	command.Flags().StringVar(&toTopic, "to-topic", "", "Destination topic")

	command.Flags().IntVar(&maxWaitingSecondsForNewMessage, "max-waiting-seconds-for-new-message", 30, "Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever.")

	command.MarkFlagsRequiredTogether("from-kafka-username", "from-kafka-password", "from-kafka-sasl-mechanism", "from-kafka-security-protocol")
	command.MarkFlagsRequiredTogether("from-kafka-username", "from-kafka-password", "from-kafka-sasl-mechanism", "from-kafka-security-protocol")
	command.MarkFlagsRequiredTogether("from-topic", "from-kafka-servers", "from-kafka-group-id")
	command.MarkFlagsRequiredTogether("to-topic", "to-kafka-servers")

	err := command.MarkFlagRequired("from-topic")
	if err != nil {
		return nil, err
	}
	err = command.MarkFlagRequired("to-topic")
	if err != nil {
		return nil, err
	}
	return &command, nil
}
