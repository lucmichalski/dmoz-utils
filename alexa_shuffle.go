package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
)

func main() {
	// open flat-sitemap file
	file, err := os.Open("alexa_links.csv")
	if err != nil {
		log.Fatal(err)
	}

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.LazyQuotes = true

	data, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	shuffle(data)
	for _, entry := range data {
		fmt.Println(entry[0])
	}

}

func shuffle(slice interface{}) {
	rv := reflect.ValueOf(slice)
	swap := reflect.Swapper(slice)
	length := rv.Len()
	for i := length - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		swap(i, j)
	}
}
