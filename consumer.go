// 2020/10/06

package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	HOST  = "10.2.152.95"
	TOPIC = "SKDB.public.sdcocdmst"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  HOST,
		"group.id":           "TEST",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
	})

	if err != nil {
		panic(err)
	}

	err = consumer.SubscribeTopics([]string{TOPIC}, nil)

	if err != nil {
		panic(err)
	}

	for {
		message, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf(
				"Received message Topic: %s Message: %s \n",
				message.TopicPartition,
				string(message.Value),
			)
		} else {
			fmt.Printf(
				"Consumer error Topic: %s \n",
				message.TopicPartition,
			)
		}
	}
}

// End
