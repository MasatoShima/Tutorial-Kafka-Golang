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
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

import (
	"github.com/linkedin/goavro/v2"
)

const (
	topic = "SKDB.public.sdcocdmst"
)

func main() {
	// Convert binary data (avro format) to Golang form data
	convertNativeFromBinary()
}

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
		// Convert to map
		datum, err := ocf.Read()

		if err != nil {
			panic(err)
		}

		// fmt.Println(datum)

		// Resolve nest data
		keysList := [][]string{
			{
				"after",
				fmt.Sprintf("%s.Value", topic),
			},
			{
				"before",
				fmt.Sprintf("%s.Value", topic),
			},
		}

		child := datum

		for _, keys := range keysList {
			for _, key := range keys {
				if child != nil {
					child, _ = child.(map[string]interface{})[key]
				}
			}

			if child != nil {
				for key, value := range child.(map[string]interface{}) {
					switch value.(type) {
					case map[string]interface{}:
						for _, v := range value.(map[string]interface{}) {
							child.(map[string]interface{})[key] = v
						}
					}
				}
			}
		}

		// Convert to string
		data, err := json.Marshal(datum)
		if err != nil {
			err = errors.New("failed converting map to string")
			return
		}

		fmt.Println(fmt.Sprintf("%s", data))
	}

	return
}

// End
