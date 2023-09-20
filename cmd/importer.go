package cmd

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func CreateImportCmd() (*cobra.Command, error) {
	var messageFilePath string
	var offsetFilePath string
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
	var clientid string
	var restoreBefore string
	var restoreAfter string

	command := cobra.Command{
		Use: "import",
		Run: func(cmd *cobra.Command, args []string) {
			logger := log.WithContext(context.Background())
			logger.Infof("Input file: %s", messageFilePath)
			parquetReader, err := impl.NewParquetReader(messageFilePath, offsetFilePath, includePartitionAndOffset)
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
				GroupId:                   "",
				QueueBufferingMaxMessages: 0,
				QueuedMaxMessagesKbytes:   0,
				FetchMessageMaxBytes:      0,
				SSLCALocation:             sslCaLocation,
				SSLKeyLocation:            sslKeyLocation,
				SSLCertLocation:           sslCertLocation,
				SSLKeyPassword:            sslKeyPassword,
				EnableAutoOffsetStore:     true,
				ClientID:                  clientid,
			}
			producer, err := kafka_utils.NewProducer(kafkaProducerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Unable to create producer"))
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
						logger.Debugf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			}()
			kafkaConsumerConfig := kafka_utils.Config{
				BootstrapServers: kafkaServers,
				SecurityProtocol: kafkaSecurityProtocol,
				SASLMechanism:    kafkaSASKMechanism,
				SASLUsername:     kafkaUsername,
				SASLPassword:     kafkaPassword,
				SSLCALocation:    sslCaLocation,
				SSLKeyPassword:   sslKeyPassword,
				SSLKeyLocation:   sslKeyLocation,
				SSLCertLocation:  sslCertLocation,
			}
			importer, err := impl.NewImporter(logger, producer, deliveryChan, parquetReader, restoreBefore, restoreAfter)
			if err != nil {
				panic(errors.Wrap(err, "unable to init importer"))
			}
			err = importer.Run(kafkaConsumerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Error while running importer"))
			}
		},
	}
	command.Flags().StringVarP(&messageFilePath, "file", "f", "", "Output file path for storing message (required)")
	command.Flags().StringVarP(&offsetFilePath, "offset-file", "o", "", "Output file path for storing offset")
	command.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	command.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	command.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	command.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	command.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	command.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	command.Flags().StringVar(&sslCaLocation, "ssl-ca-location", "", "Location of client ca cert file in pem")
	command.Flags().StringVar(&sslKeyPassword, "ssl-key-password", "", "Password for ssl private key passphrase")
	command.Flags().StringVar(&sslCertLocation, "ssl-certificate-location", "", "Client's certificate location")
	command.Flags().StringVar(&sslKeyLocation, "ssl-key-location", "", "Path to ssl private key")
	command.Flags().StringVar(&clientid, "client-id", "", "Producer client id")
	command.Flags().StringVar(&restoreBefore, "restore-before", "", "timestamp in RFC3339 format to restore data before this time")
	command.Flags().StringVar(&restoreAfter, "restore-after", "", "timestamp in RFC3339 format to restore data after this time")
	command.Flags().BoolVarP(&includePartitionAndOffset, "include-partition-and-offset", "i", false, "To store partition and offset of kafka message in file")
	err := command.MarkFlagRequired("file")
	if err != nil {
		return nil, err
	}
	return &command, nil
}
