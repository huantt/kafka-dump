package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
	"sync"
	"time"
)

func main() {
	if err := run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	var logLevel string
	var filePath string
	var kafkaServers string
	var kafkaUsername string
	var kafkaPassword string
	var kafkaSecurityProtocol string
	var kafkaSASKMechanism string
	var kafkaGroupID string
	var topics *[]string
	rootCmd := &cobra.Command{}
	rootCmd.PersistentFlags().StringVarP(&logLevel, "log-level", "", "info", "Log level")
	logConfig := log.Config{Level: logLevel, Format: "text"}
	logConfig.Build()

	// Init count parquet number of rows command
	countParquetNumberOfRowsCmd := cobra.Command{
		Use: "count-parquet-rows",
		Run: func(cmd *cobra.Command, args []string) {
			parquetReader, err := impl.NewParquetReader(filePath)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init parquet file reader"))
			}
			log.Infof("Number of rows: %d", parquetReader.GetNumberOfRows())
		},
	}
	countParquetNumberOfRowsCmd.Flags().StringVarP(&filePath, "file", "f", "", "File path (required)")
	err := countParquetNumberOfRowsCmd.MarkFlagRequired("file")
	if err != nil {
		return err
	}

	// Init export command
	var exportLimitPerFile uint64
	var maxWaitingSecondsForNewMessage int
	var concurrentConsumers = 1
	exportCmd := cobra.Command{
		Use: "export",
		Run: func(cmd *cobra.Command, args []string) {
			log.Infof("Limit: %d - Concurrent consumers: %d", exportLimitPerFile, concurrentConsumers)
			kafkaConsumerConfig := kafka_utils.Config{
				BootstrapServers: kafkaServers,
				SecurityProtocol: kafkaSecurityProtocol,
				SASLMechanism:    kafkaSASKMechanism,
				SASLUsername:     kafkaUsername,
				SASLPassword:     kafkaPassword,
				GroupId:          kafkaGroupID,
			}
			consumer, err := kafka_utils.NewConsumer(kafkaConsumerConfig)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init consumer"))
			}
			maxWaitingTimeForNewMessage := time.Duration(maxWaitingSecondsForNewMessage) * time.Second
			options := &impl.Options{
				Limit:                       exportLimitPerFile,
				MaxWaitingTimeForNewMessage: &maxWaitingTimeForNewMessage,
			}

			var wg sync.WaitGroup
			wg.Add(concurrentConsumers)
			for i := 0; i < concurrentConsumers; i++ {
				go func(workerID int) {
					defer wg.Done()
					for true {
						outputFilePath := filePath
						if exportLimitPerFile > 0 {
							outputFilePath = fmt.Sprintf("%s.%d", filePath, time.Now().UnixMilli())
						}
						log.Infof("[Worker-%d] Exporting to: %s", workerID, outputFilePath)
						parquetWriter, err := impl.NewParquetWriter(outputFilePath)
						if err != nil {
							panic(errors.Wrap(err, "Unable to init parquet file writer"))
						}
						exporter, err := impl.NewExporter(consumer, *topics, parquetWriter, options)
						if err != nil {
							panic(errors.Wrap(err, "Failed to init exporter"))
						}

						exportedCount, err := exporter.Run()
						if err != nil {
							panic(errors.Wrap(err, "Error while running exporter"))
						}
						log.Infof("[Worker-%d] Exported %d messages", workerID, exportedCount)
						if exportLimitPerFile == 0 || exportedCount < exportLimitPerFile {
							log.Infof("[Worker-%d] Finished!", workerID)
							return
						}
					}
				}(i)
			}
			wg.Wait()
		},
	}
	exportCmd.Flags().StringVarP(&filePath, "file", "f", "", "Output file path (required)")
	exportCmd.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	exportCmd.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	exportCmd.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	exportCmd.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	exportCmd.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	exportCmd.Flags().StringVar(&kafkaGroupID, "kafka-group-id", "", "Kafka consumer group ID")
	exportCmd.Flags().Uint64Var(&exportLimitPerFile, "limit", 0, "Supports file splitting. Files are split by the number of messages specified")
	exportCmd.Flags().IntVar(&maxWaitingSecondsForNewMessage, "max-waiting-seconds-for-new-message", 30, "Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever.")
	exportCmd.Flags().IntVar(&concurrentConsumers, "concurrent-consumers", 1, "Number of concurrent consumers")
	topics = exportCmd.Flags().StringArray("kafka-topics", nil, "Kafka topics")
	exportCmd.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	err = exportCmd.MarkFlagRequired("file")
	if err != nil {
		return err
	}

	// Init import command
	importCmd := cobra.Command{
		Use: "import",
		Run: func(cmd *cobra.Command, args []string) {
			log.Infof("Input file: %s", filePath)
			parquetReader, err := impl.NewParquetReader(filePath)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init parquet file reader"))
			}
			kafkaProducerConfig := kafka_utils.Config{
				BootstrapServers: kafkaServers,
				SecurityProtocol: kafkaSecurityProtocol,
				SASLMechanism:    kafkaSASKMechanism,
				SASLUsername:     kafkaUsername,
				SASLPassword:     kafkaPassword,
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
						log.Debugf("Successfully produced record to topic %s partition [%d] @ offset %v\n",
							*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
					}
				}
			}()
			importer := impl.NewImporter(producer, deliveryChan, parquetReader)
			if err != nil {
				panic(errors.Wrap(err, "Unable to init importer"))
			}

			err = importer.Run()
			if err != nil {
				panic(errors.Wrap(err, "Error while running importer"))
			}
		},
	}
	importCmd.Flags().StringVarP(&filePath, "file", "f", "", "Output file path (required)")
	importCmd.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	importCmd.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	importCmd.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	importCmd.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	importCmd.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	importCmd.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	if err != nil {
		return err
	}

	// Init stream command
	var topicFrom string
	var topicTo string
	streamCmd := cobra.Command{
		Use: "stream",
		Run: func(cmd *cobra.Command, args []string) {
			//TODO: Separate consumer & producer config
			kafkaConsumerConfig := kafka_utils.Config{
				BootstrapServers: kafkaServers,
				SecurityProtocol: kafkaSecurityProtocol,
				SASLMechanism:    kafkaSASKMechanism,
				SASLUsername:     kafkaUsername,
				SASLPassword:     kafkaPassword,
				GroupId:          kafkaGroupID,
			}
			consumer, err := kafka_utils.NewConsumer(kafkaConsumerConfig)
			if err != nil {
				panic(err)
			}
			kafkaProducerConfig := kafka_utils.Config{
				BootstrapServers: kafkaServers,
				SecurityProtocol: kafkaSecurityProtocol,
				SASLMechanism:    kafkaSASKMechanism,
				SASLUsername:     kafkaUsername,
				SASLPassword:     kafkaPassword,
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
				topicFrom,
				topicTo,
				deliveryChan,
				impl.StreamerOptions{MaxWaitingTimeForNewMessage: &maxWaitingSecondsForNewMessage},
			)
			transferredCount, err := streamer.Run()
			if err != nil {
				panic(err)
			}
			log.Infof("Transferred %d messages from %s to %s", transferredCount, topicFrom, topicTo)
		},
	}
	streamCmd.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	streamCmd.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	streamCmd.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	streamCmd.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	streamCmd.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	streamCmd.Flags().StringVar(&kafkaGroupID, "kafka-group-id", "", "Kafka consumer group ID")
	streamCmd.Flags().StringVar(&topicFrom, "topic-from", "", "Source topic")
	streamCmd.Flags().StringVar(&topicTo, "topic-to", "", "Destination topic")
	streamCmd.Flags().IntVar(&maxWaitingSecondsForNewMessage, "max-waiting-seconds-for-new-message", 30, "Max waiting seconds for new message, then this process will be marked as finish. Set -1 to wait forever.")
	streamCmd.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")

	err = streamCmd.MarkFlagRequired("topic-from")
	if err != nil {
		return err
	}
	err = streamCmd.MarkFlagRequired("topic-to")
	if err != nil {
		return err
	}

	rootCmd.AddCommand(
		&exportCmd,
		&importCmd,
		&countParquetNumberOfRowsCmd,
		&streamCmd,
	)

	return rootCmd.Execute()
}
