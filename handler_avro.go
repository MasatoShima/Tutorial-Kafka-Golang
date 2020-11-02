/*
Name: handler_avro.go
Created by: Masato Shima
Created on: 2020/10/13
Description:
Apache avro sample
*/

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

import (
	"github.com/linkedin/goavro"
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
	parseSchemaInfo(schema)
}

func readSchemaFile() []byte {
	// Read schema file
	file, err := ioutil.ReadFile("avro/schema/schema-SKDB.public.sdcocdmst.json")

	if err != nil {
		panic(err)
	}

	fmt.Printf("%s\n", file)

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

	fmt.Println(codec)

	return codec
}

// End
