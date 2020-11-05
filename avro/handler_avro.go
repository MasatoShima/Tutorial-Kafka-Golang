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
		datum, err := ocf.Read()

		if err != nil {
			panic(err)
		}

		fmt.Println(datum)
	}

	return
}

// End
