// 2020/10/06

package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	topic := "quickstart-events"

	consumer.SubscribeTopics([]string{topic}, nil)

	for {
		message, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf(
				"Consumer error Topic: %s \n",
				message.TopicPartition,
			)
		} else {
			fmt.Printf(
				"Received message Topic: %s Message: %s \n",
				message.TopicPartition,
				string(message.Value),
			)
		}
	}

}
