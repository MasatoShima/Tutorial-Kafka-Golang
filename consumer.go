// 2020/10/06

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const (
	host  = "10.2.151.196"
	port  = "9092"
	topic = "SKDB.public.sdmstbas"
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
		fmt.Println("Start subscribe topic")

		message, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf(
				"Received message Topic: %v \n",
				message.TopicPartition.Topic,
			)

			err = writeMessageValue(message)

			if err != nil {
				panic(err)
			}

			err = convertNativeFromBinary(codec, message)

			if err != nil {
				panic(err)
			}

		} else if message == nil {
			fmt.Printf(
				"No received message Topic: %s \n",
				topic,
			)

			continue
		} else {
			fmt.Printf(
				"Consumer error Topic: %s \n",
				message.TopicPartition,
			)
		}
		break
	}
}

func readSchemaFile() []byte {
	// Read schema file
	file, err := ioutil.ReadFile(
		fmt.Sprintf("avro/schema/schema-%s.json", topic),
	)

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

func writeMessageValue(message *kafka.Message) error {
	err := ioutil.WriteFile(
		fmt.Sprintf("avro/avro-%s.avro", topic),
		message.Value,
		777,
	)

	if err != nil {
		return err
	}

	return nil
}

func convertNativeFromBinary(codec *goavro.Codec, message *kafka.Message) error {
	// Convert binary data (avro format) to Golang form data
	native, _, err := codec.NativeFromBinary(message.Value[5:])

	if err != nil {
		return err
	}

	fmt.Println(native)

	return nil
}

// End
