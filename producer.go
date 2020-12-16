// 2020/10/05

package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
	})

	if err != nil {
		panic(err)
	}

	defer producer.Close()

	// Delivery report handler for produced messages
	go func() {
		for event := range producer.Events() {
			switch e := event.(type) {
			case *kafka.Message:
				if e.TopicPartition.Error == nil {
					fmt.Printf(
						"Delivery success Topic: %s Message: %s \n",
						e.TopicPartition,
						e.Value,
					)
				} else {
					fmt.Printf(
						"Delivery failed Topic: %s \n ",
						e.TopicPartition,
					)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "quickstart-events"

	for _, message := range []string{"message1", "message2", "message3"} {
		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)

		if err != nil {
			panic(err)
		}
	}

	producer.Flush(15 * 1000)
}

// End
