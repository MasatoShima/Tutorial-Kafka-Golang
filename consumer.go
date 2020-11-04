// 2020/10/06

package main

import (
	"bytes"
	"fmt"
)

import (
	"github.com/linkedin/goavro/v2"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	host  = "10.2.152.95"
	port  = "9092"
	topic = "SKDB.public.sdcocdmst"
)

type Schema struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Id      int    `json:"id"`
	Schema  string `json:"schema"`
}

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  fmt.Sprintf("%s:%s", host, port),
		"group.id":           "TEST",
		"enable.auto.commit": false,
		"auto.offset.reset":  "earliest",
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

			convertNativeFromBinary(message.Value)

		} else if message == nil {
			continue

		} else {
			fmt.Printf(
				"Consumer error Topic: %s \n",
				message.TopicPartition,
			)

		}
	}
}

func convertNativeFromBinary(messageValue []byte) {
	// Convert binary data (avro format) to Golang form data
	ocf, err := goavro.NewOCFReader(bytes.NewReader(messageValue))

	if err != nil {
		panic(err)
	}

	if ocf == nil {
		fmt.Println("Skip processing, because empty data...")
		return
	}

	for ocf.Scan() {
		datum, err := ocf.Read()

		if err != nil {
			panic(err)
		}

		fmt.Println(datum)
	}
}

// End
