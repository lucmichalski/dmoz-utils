package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	ccsv "github.com/lucmichalski/dmoz-utils/pkg/csv"
)

func main() {

	// create dump file
	csvTopLevel, err := ccsv.NewCsvWriter("../gdrive/dmoz/dmoz_toplevel.csv")
	check(err)
	defer csvTopLevel.Close()

	// open flat-sitemap file
	file, err := os.Open("../gdrive/dmoz/content.csv")
	check(err)

	reader := csv.NewReader(file)
	reader.Comma = ','
	reader.LazyQuotes = true

	data, err := reader.ReadAll()
	check(err)

	for _, entry := range data {
		cats := strings.Split(entry[3], "/")
		if len(cats) < 2 {
			continue
		}
		entry[3] = cats[1]
		fmt.Println("topLevel:", cats[1])
		if cats[1] == "Regional" || cats[1] == "World" {
			fmt.Println("skipping:", cats[1])
		} else {
			csvTopLevel.Write(entry)
			csvTopLevel.Flush()
		}
	}

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
