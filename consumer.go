// 2020/10/06

package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	host         = "10.2.152.95"
	portBrokers  = "9092"
	portRegistry = "8081"
	topic        = "SKDB.public.sdcocdmst"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":   fmt.Sprintf("%s:%s", host, portBrokers),
		"group.id":            "TEST",
		"enable.auto.commit":  false,
		"auto.offset.reset":   "earliest",
		"schema.registry.url": fmt.Sprintf("http://%s:%s", host, portRegistry),
	})

	if err != nil {
		panic(err)
	}

	err = consumer.SubscribeTopics([]string{topic}, nil)

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
