package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	config "github.com/huantt/kafka-dump/cfg"
	"github.com/huantt/kafka-dump/impl"
	"github.com/huantt/kafka-dump/pkg/kafka_utils"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

func main() {
	if err := run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	cfg.Log.Build()

	cmd := &cobra.Command{}
	cmd.AddCommand(
		&cobra.Command{
			Use: "export",
			Run: func(cmd *cobra.Command, args []string) {
				log.Infof("Output destination: %s", cfg.OutPath)
				parquetWriter, err := impl.NewParquetWriter(cfg.OutPath)
				if err != nil {
					panic(errors.Wrap(err, "Unable to init parquet file writer"))
				}

				consumer, err := kafka_utils.NewConsumer(cfg.Kafka)
				if err != nil {
					panic(errors.Wrap(err, "Unable to init consumer"))
				}
				exporter, err := impl.NewExporter(consumer, strings.Split(cfg.Topics, ","), parquetWriter)
				if err != nil {
					panic(errors.Wrap(err, "Failed to init exporter"))
				}

				err = exporter.Run()
				if err != nil {
					panic(errors.Wrap(err, "Error while running exporter"))
				}
			},
		},
		&cobra.Command{
			Use: "import",
			Run: func(cmd *cobra.Command, args []string) {
				log.Infof("Input file: %s", cfg.InputPath)
				parquetReader, err := impl.NewParquetReader(cfg.OutPath)
				if err != nil {
					panic(errors.Wrap(err, "Unable to init parquet file reader"))
				}

				producer, err := kafka_utils.NewProducer(cfg.Kafka)
				if err != nil {
					panic(errors.Wrap(err, "Unable to create producer"))
				}
				queueBufferingMaxMessages := kafka_utils.DefaultQueueBufferingMaxMessages
				if cfg.Kafka.QueueBufferingMaxMessages > 0 {
					queueBufferingMaxMessages = cfg.Kafka.QueueBufferingMaxMessages
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
		},
	)

	return cmd.Execute()
}
