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
	//"github.com/linkedin/goavro"
)

func main() {
	// Read json file
	file, errReadFile := ioutil.ReadFile("data/sample_1.json")

	if errReadFile != nil {
		panic(errReadFile)
	}

	fmt.Printf("%s", file)

	// Encode json file
	var jsonData interface{}

	errJsonUnmarshal := json.Unmarshal(file, &jsonData)

	if errJsonUnmarshal != nil {
		panic(errJsonUnmarshal)
	}

	fmt.Printf("%s", jsonData)
}

// End
