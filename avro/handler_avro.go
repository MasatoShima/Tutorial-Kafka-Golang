/*
 * Name: handler_avro.go
 * Created by: Masato Shima
 * Created on: 2020/10/13
 * Description:
 * Apache avro sample
 */

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
)

const (
	topic = "SKDB.public.sdmstmkt"
)

type Schema struct {
	Subject string `json:"subject"`
	Version int    `json:"version"`
	Id      int    `json:"id"`
	Schema  string `json:"schema"`
}

func main() {
	// Read schema file
	file := readSchemaFile()

	// Fetch schema info
	schema := fetchSchemaInfo(file)

	// Parse avro schema
	codec := parseSchemaInfo(schema)

	// Convert binary data (avro format) to Golang form data
	convertNativeFromBinary(codec)
}

func readSchemaFile() []byte {
	// Read schema file
	file, err := ioutil.ReadFile(
		fmt.Sprintf("schema/schema-%s.json", topic),
	)

	if err != nil {
		panic(err)
	}

	return file
}

func parseSchemaInfo(schemaInfo string) *goavro.Codec {
	// Parse avro schema
	codec, err := goavro.NewCodec(schemaInfo)

	if err != nil {
		panic(err)
	}

	return codec
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

func convertNativeFromBinary(codec *goavro.Codec) {
	// Convert binary data (avro format) to Golang form data
	file, err := ioutil.ReadFile(fmt.Sprintf("data/avro-%s.avro", topic))

	if err != nil {
		panic(err)
	}

	// Convert binary data (avro format) to Golang form data
	native, _, err := codec.NativeFromBinary(file[5:])

	if err != nil {
		panic(err)
	}

	fmt.Println(native)

	return
}

// End
