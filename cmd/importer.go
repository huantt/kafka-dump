package cmd

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func CreateImportCmd() (*cobra.Command, error) {
	var filePath string
	var kafkaServers string
	var kafkaUsername string
	var kafkaPassword string
	var kafkaSecurityProtocol string
	var kafkaSASKMechanism string
	var sslCaLocation string
	var sslKeyPassword string
	var sslCertLocation string
	var sslKeyLocation string
	var includePartitionAndOffset bool
	var enableAutoOffsetStore = true
	var queueBufferingMaxMessages int

	command := cobra.Command{
		Use: "import",
		Run: func(cmd *cobra.Command, args []string) {
			log.Infof("Input file: %s", filePath)
			parquetReader, err := impl.NewParquetReader(filePath, includePartitionAndOffset)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init parquet file reader"))
			}
			kafkaProducerConfig := kafka_utils.Config{
				BootstrapServers:          kafkaServers,
				SecurityProtocol:          kafkaSecurityProtocol,
				SASLMechanism:             kafkaSASKMechanism,
				SASLUsername:              kafkaUsername,
				SASLPassword:              kafkaPassword,
				ReadTimeoutSeconds:        0,
				QueueBufferingMaxMessages: queueBufferingMaxMessages,
				QueuedMaxMessagesKbytes:   0,
				FetchMessageMaxBytes:      0,
				SSLCALocation:             sslCaLocation,
				SSLKeyLocation:            sslKeyLocation,
				SSLCertLocation:           sslCertLocation,
				SSLKeyPassword:            sslKeyPassword,
				EnableAutoOffsetStore:     enableAutoOffsetStore,
			}
			producer, err := kafka_utils.NewProducer(kafkaProducerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Unable to create producer"))
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
			importer := impl.NewImporter(producer, deliveryChan, parquetReader)
			err = importer.Run()
			if err != nil {
				panic(errors.Wrap(err, "Error while running importer"))
			}
		},
	}
	command.Flags().StringVarP(&filePath, "file", "f", "", "Output file path (required)")
	command.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	command.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	command.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	command.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	command.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	command.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	command.Flags().StringVar(&sslCaLocation, "ssl-ca-location", "", "location of client ca cert file in pem")
	command.Flags().StringVar(&sslKeyPassword, "ssl-key-password", "", "password for ssl private key passphrase")
	command.Flags().StringVar(&sslCertLocation, "ssl-certificate-location", "", "client's certificate location")
	command.Flags().StringVar(&sslKeyLocation, "ssl-key-location", "", "path to ssl private key")
	command.Flags().BoolVarP(&includePartitionAndOffset, "include-partition-and-offset", "i", false, "to store partition and offset of kafka message in file")
	command.Flags().BoolVar(&enableAutoOffsetStore, "enable-auto-offset-store", true, "to store offset in kafka broker")
	command.Flags().IntVar(&queueBufferingMaxMessages, "queue-buffering-max-messages", kafka_utils.DefaultQueueBufferingMaxMessages, "queue buffering max messages")
	return &command, nil
}
