// 2020/10/05

package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func main() {
	producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
}
