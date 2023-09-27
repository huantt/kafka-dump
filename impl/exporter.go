package impl

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/huantt/kafka-dump/pkg/log"
	"github.com/pkg/errors"
)

type Exporter struct {
	admin    *kafka.AdminClient
	consumer *kafka.Consumer
	topics   []string
	writer   Writer
	options  *Options
}

func NewExporter(adminClient *kafka.AdminClient, consumer *kafka.Consumer, topics []string, writer Writer, options *Options) (*Exporter, error) {
	return &Exporter{
		admin:    adminClient,
		consumer: consumer,
		topics:   topics,
		writer:   writer,
		options:  options,
	}, nil
}

type Options struct {
	Limit                       uint64
	MaxWaitingTimeForNewMessage *time.Duration
}

type Writer interface {
	Write(msg kafka.Message) error
	OffsetWrite(msg kafkaOffsetMessage) error
	Flush() error
}

const defaultMaxWaitingTimeForNewMessage = time.Duration(30) * time.Second

func (e *Exporter) Run() (exportedCount uint64, err error) {
	err = e.consumer.SubscribeTopics(e.topics, nil)
	if err != nil {
		return
	}
	log.Infof("Subscribed topics: %s", e.topics)
	cx := make(chan os.Signal, 1)
	signal.Notify(cx, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-cx
		err = e.flushData()
		if err != nil {
			panic(err)
		}
		os.Exit(1)
	}()
	defer func() {
		err = e.flushData()
		if err != nil {
			panic(err)
		}
	}()
	maxWaitingTimeForNewMessage := defaultMaxWaitingTimeForNewMessage
	if e.options.MaxWaitingTimeForNewMessage != nil {
		maxWaitingTimeForNewMessage = *e.options.MaxWaitingTimeForNewMessage
	}
	for {
		msg, err := e.consumer.ReadMessage(maxWaitingTimeForNewMessage)
		if err != nil {
			if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
				log.Infof("Waited for %s but no messages any more! Finish!", maxWaitingTimeForNewMessage)
			}
			return exportedCount, err
		}
		err = e.writer.Write(*msg)
		if err != nil {
			return exportedCount, err
		}
		exportedCount++
		log.Infof("Exported message: %v (Total: %d)", msg.TopicPartition, exportedCount)
		if e.options != nil && exportedCount == e.options.Limit {
			log.Infof("Reached limit %d - Finish!", e.options.Limit)
			return exportedCount, err
		}
	}
}

func (e *Exporter) flushData() error {
	err := e.StoreConsumerGroupOffset()
	if err != nil {
		panic(errors.Wrap(err, "unable to read consumer group"))
	}
	err = e.writer.Flush()
	if err != nil {
		return errors.Wrap(err, "Failed to flush writer")
	}
	_, err = e.consumer.Commit()
	if err != nil {
		if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrNoOffset {
			log.Warnf("No offset, it can happen when there is no message to read, error is: %v", err)
		} else {
			return errors.Wrap(err, "Failed to commit messages")
		}
	}
	return nil
}

func (e *Exporter) StoreConsumerGroupOffset() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// state, err := kafka.ConsumerGroupStateFromString("stable")
	// if err != nil {
	// 	return exportedCount, err
	// }
	// listRes, err := e.admin.ListConsumerGroups(ctx, kafka.SetAdminRequestTimeout(5*time.Second), kafka.SetAdminMatchConsumerGroupStates([]kafka.ConsumerGroupState{state}))
	// if err != nil {
	// 	return exportedCount, errors.Wrap(err, "unable to list consumer groups")
	// }

	listRes, err := e.admin.ListConsumerGroups(ctx, kafka.SetAdminRequestTimeout(5*time.Second))
	if err != nil {
		return errors.Wrap(err, "unable to list consumer groups")
	}

	groupIds := make([]string, 0)
	topicTogroupNameList := make(map[string]map[string]struct{}, 0)
	consumerGroupList := make(map[string]struct{}, 0)
	for _, v := range listRes.Valid {
		groupIds = append(groupIds, v.GroupID)
	}
	log.Infof("List of consumer groups is: %v", groupIds)

	if len(groupIds) > 0 {
		groupRes, err := e.admin.DescribeConsumerGroups(ctx, groupIds, kafka.SetAdminRequestTimeout(5*time.Second))
		if err != nil {
			return errors.Wrapf(err, "unable to describe consumer groups %v", groupIds)
		}
		log.Infof("group result is: %v", groupRes)

		// TODO: improve the complexity
		for _, groupDescription := range groupRes.ConsumerGroupDescriptions {
			log.Infof("group description is: %v", groupDescription)
			for _, member := range groupDescription.Members {
				log.Infof("member is: %v", member)
				for _, groupTopic := range member.Assignment.TopicPartitions {
					log.Infof("group topic is: %s", *groupTopic.Topic)
					for _, topic := range e.topics {
						log.Infof("topic is: %s", topic)
						// Matching the topic with the provided topic
						// as we don't want to store offset for all the
						// consumer group
						if *groupTopic.Topic == topic {
							consumerGroupList[groupDescription.GroupID] = struct{}{}
						}
					}
					topicTogroupNameList[*groupTopic.Topic] = consumerGroupList
				}
			}
		}
	}

	topicToPartitionList := make(map[string]kafka.TopicPartitions, 0)
	kafkaTopicPartitions := make(kafka.TopicPartitions, 0)
	for _, topic := range e.topics {
		// get metadata of a topic
		metadata, err := e.consumer.GetMetadata(&topic, false, 5000)
		if err != nil {
			log.Errorf("error in getting metadata of topic %s: %w", topic, err)
			return errors.Wrapf(err, "unable to get metadata of a topic: %s", topic)
		}
		log.Debugf("metadata is: %v", metadata)
		topicMetadata := metadata.Topics[topic]
		log.Infof("topic metadata is: %v", topicMetadata)
		topicPartition := topicMetadata.Partitions
		for _, partition := range topicPartition {
			log.Debugf("partition id is: %v", partition.ID)
			var ktp kafka.TopicPartition
			ktp.Topic = &topic
			ktp.Partition = partition.ID
			kafkaTopicPartitions = append(kafkaTopicPartitions, ktp)
		}
		topicToPartitionList[topic] = kafkaTopicPartitions
	}

	log.Debugf("topic to partition is: %v", topicToPartitionList)
	log.Debugf("group name list is: %v", topicTogroupNameList)

	for _, topic := range e.topics {
		groupList := topicTogroupNameList[topic]
		for k := range groupList {
			kafkaTopicPartitionsForFinal := make([]kafkaTopicPartition, 0)
			groupTopicPartitions := make([]kafka.ConsumerGroupTopicPartitions, 0)
			kafkaTopicPartitions := topicToPartitionList[topic]
			// Get the watermark for each partition of a topic
			// and create a datastructure to store the values
			for _, partition := range kafkaTopicPartitions {
				// This is to store the watermark offset high and low value
				var store kafkaTopicPartition
				low, high, err := e.consumer.GetWatermarkOffsets(topic, partition.Partition)
				if err != nil {
					log.Errorf("error in getting watermark offset of topic %s, partition %d: %w", topic, partition.Partition, err)
					return errors.Wrapf(err, "unable to get metadata of a topic: %s, partition %d", topic, partition.Partition)
				}
				store.WOH = high
				store.WOL = low
				store.TopicPartition = partition
				kafkaTopicPartitionsForFinal = append(kafkaTopicPartitionsForFinal, store)
			}
			log.Infof("kafkaTopicPartitionsForFinal is: %v", kafkaTopicPartitionsForFinal)
			l := len(kafkaTopicPartitionsForFinal)
			log.Infof("length of kafkaTopicPartitionsForFinal is: %d", l)
			// required to fetch the current offset of the consumer group
			groupTopicPartition := kafka.ConsumerGroupTopicPartitions{
				Group:      k,
				Partitions: kafkaTopicPartitions,
			}
			groupTopicPartitions = append(groupTopicPartitions, groupTopicPartition)
			log.Infof("groupTopicPartitions is: %v", groupTopicPartitions)
			lcgor, err := e.admin.ListConsumerGroupOffsets(ctx, groupTopicPartitions, kafka.SetAdminRequireStableOffsets(false))
			if err != nil {
				return errors.Wrapf(err, "unable to list consumer groups offsets %v", groupTopicPartitions)
			}
			var finalRes kafkaOffsetMessage
			// matching the topic partition and updating the offset with current offset
			for _, res := range lcgor.ConsumerGroupsTopicPartitions {
				log.Infof("consumer group topic paritions is %v", res.String())
				finalRes.Group = res.Group
				for i, customKafkaTopicPartition := range kafkaTopicPartitionsForFinal {
					for _, kafkaTopicPartition := range res.Partitions {
						if customKafkaTopicPartition.Partition == kafkaTopicPartition.Partition && *customKafkaTopicPartition.Topic == *kafkaTopicPartition.Topic {
							log.Infof("comparing kafka topic partition: %v, %v", customKafkaTopicPartition.Partition, kafkaTopicPartition.Partition)
							log.Infof("comparing kafka topic: %v, %v", *customKafkaTopicPartition.Topic, *kafkaTopicPartition.Topic)
							kafkaTopicPartitionsForFinal[i].Offset = kafkaTopicPartition.Offset
						}
					}
				}
				log.Infof("final Result before substitution: %v", finalRes)
				finalRes.KafkaTopicPartitions = kafkaTopicPartitionsForFinal
				log.Infof("final Result after substitution: %v", finalRes)
				err := e.writer.OffsetWrite(finalRes)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
