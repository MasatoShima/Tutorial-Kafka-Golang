/*
 * Name: handler_avro.go
 * Created by: Masato Shima
 * Created on: 2020/10/13
 * Description:
 * Apache avro sample
 */

package main

import (
	"bufio"
	"fmt"
	"os"
)

import (
	"github.com/linkedin/goavro/v2"
)

// type Schema struct {
// 	Subject string `json:"subject"`
// 	Version int    `json:"version"`
// 	Id      int    `json:"id"`
// 	Schema  string `json:"schema"`
// }

func main() {
	// // Read schema file
	// file := readSchemaFile()
	//
	// // Fetch schema info
	// schema := fetchSchemaInfo(file)
	//
	// // Parse avro schema
	// codec := parseSchemaInfo(schema)
	//
	// // Read avro file
	// avro := readAvroFile()

	// Convert binary data (avro format) to Golang form data
	convertNativeFromBinary()
}

// func readSchemaFile() []byte {
// 	// Read schema file
// 	file, err := ioutil.ReadFile("schema/schema-SKDB.public.sdcocdmst.json")
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	return file
// }

// func fetchSchemaInfo(file []byte) string {
// 	// Fetch schema info
// 	var jsonData Schema
//
// 	err := json.Unmarshal(file, &jsonData)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Printf("%s\n", jsonData.Schema)
//
// 	return jsonData.Schema
// }

// func parseSchemaInfo(schemaInfo string) *goavro.Codec {
// 	// Parse avro schema
// 	codec, err := goavro.NewCodec(schemaInfo)
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	return codec
// }

// func readAvroFile() []byte {
// 	// Read avro file
// 	file, err := ioutil.ReadFile("avro-SKDB.public.sdcocdmst.avro")
//
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	fmt.Println(file)
//
// 	return file
// }

func convertNativeFromBinary() {
	// Convert binary data (avro format) to Golang form data
	file, err := os.Open("avro-SKDB.public.sdcocdmst.avro")

	if err != nil {
		panic(err)
	}

	ocf, err := goavro.NewOCFReader(bufio.NewReader(file))

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

	return
}

// End
