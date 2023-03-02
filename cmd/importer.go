package cmd

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"io/ioutil"
)

func CreateImportCmd() (*cobra.Command, error) {
	var filePath string
	var folder string
	var kafkaServers string
	var kafkaUsername string
	var kafkaPassword string
	var kafkaSecurityProtocol string
	var kafkaSASKMechanism string

	command := cobra.Command{
		Use: "import",
		Run: func(cmd *cobra.Command, args []string) {
			if folder == "" && filePath == "" {
				panic(errors.New("Input file or folder is required"))
			}
			log.Infof("Input file: %s", filePath)
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
			var filePaths []string
			if folder != "" {
				files, err := ioutil.ReadDir(folder)
				if err != nil {
					panic(err)
				}
				for _, file := range files {
					if file.IsDir() {
						continue
					}
					filePaths = append(filePaths, fmt.Sprintf("%s/%s", folder, file.Name()))
				}
			}
			for _, path := range filePaths {
				log.Infof("Reading file: %s", path)
				parquetReader, err := impl.NewParquetReader(path)
				if err != nil {
					panic(errors.Wrap(err, "Unable to init parquet file reader"))
				}
				importer := impl.NewImporter(producer, deliveryChan, parquetReader)
				if err != nil {
					panic(errors.Wrap(err, "Unable to init importer"))
				}
				err = importer.Run()
				if err != nil {
					panic(errors.Wrap(err, "Error while running importer"))
				}
			}
		},
	}
	command.Flags().StringVarP(&filePath, "file", "f", "", "Input file path (file or folder is required)")
	command.Flags().StringVar(&folder, "folder", "", "Input folder path (file or folder is required)")
	command.Flags().StringVar(&kafkaServers, "kafka-servers", "", "Kafka servers string")
	command.Flags().StringVar(&kafkaUsername, "kafka-username", "", "Kafka username")
	command.Flags().StringVar(&kafkaPassword, "kafka-password", "", "Kafka password")
	command.Flags().StringVar(&kafkaSASKMechanism, "kafka-sasl-mechanism", "", "Kafka password")
	command.Flags().StringVar(&kafkaSecurityProtocol, "kafka-security-protocol", "", "Kafka security protocol")
	command.MarkFlagsRequiredTogether("kafka-username", "kafka-password", "kafka-sasl-mechanism", "kafka-security-protocol")
	return &command, nil
}
