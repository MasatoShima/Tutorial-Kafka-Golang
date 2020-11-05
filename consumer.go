// 2020/10/06

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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

	// Read schema file
	file := readSchemaFile()

	// Fetch schema info
	schema := fetchSchemaInfo(file)

	// Parse avro schema
	codec := parseSchemaInfo(schema)

	for {
		message, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf(
				"Received message Topic: %s Message: %s \n",
				message.TopicPartition,
				string(message.Value),
			)

			convertNativeFromBinary(codec, message)

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

func readSchemaFile() []byte {
	// Read schema file
	file, err := ioutil.ReadFile("avro/schema/schema-SKDB.public.sdcocdmst.json")

	if err != nil {
		panic(err)
	}

	return file
}

func fetchSchemaInfo(file []byte) string {
	// Fetch schema info
	var jsonData Schema

	err := json.Unmarshal(file, &jsonData)

	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", jsonData.Schema)

	return jsonData.Schema
}

func parseSchemaInfo(schemaInfo string) *goavro.Codec {
	// Parse avro schema
	codec, err := goavro.NewCodec(schemaInfo)

	if err != nil {
		panic(err)
	}

	return codec
}

func convertNativeFromBinary(codec *goavro.Codec, message *kafka.Message) {
	// Convert binary data (avro format) to Golang form data
	native, _, err := codec.NativeFromBinary(message.Value[5:])

	if err != nil {
		panic(err)
	}

	fmt.Println(native)
}

// End
