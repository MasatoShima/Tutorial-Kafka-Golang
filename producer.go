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
					fmt.Println("Delivery success \n ", e.TopicPartition)
				} else {
					fmt.Println("Delivery failed \n ", e.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "TopicName"

	for _, message := range []string{"message1", "message2", "message3"} {
		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(message),
		}, nil)
	}

	producer.Flush(15 * 1000)
}

// End
