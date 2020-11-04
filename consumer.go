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
		message := consumer.Poll(-1)

		if message != nil {
			fmt.Println("Received message")
		} else {
			fmt.Println("Not received message")
			continue
		}

		switch m := message.(type) {
		case *kafka.Message:
			fmt.Println("This is message")
			fmt.Println(m.Value)
			convertNativeFromBinary(m)
		case *kafka.Error:
			fmt.Println("Error...")
			fmt.Println(m.Code())
		default:
			fmt.Println("Skip!")
		}
	}
}

func convertNativeFromBinary(message *kafka.Message) {
	// Convert binary data (avro format) to Golang form data
	messageValue := bytes.NewReader(message.Value[5:])
	ocf, err := goavro.NewOCFReader(messageValue)

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
