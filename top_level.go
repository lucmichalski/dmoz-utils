package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/abadojack/whatlanggo"

	ccsv "github.com/lucmichalski/dmoz-utils/pkg/csv"
)

func main() {

	// create dump file
	csvTopLevel, err := ccsv.NewCsvWriter("../gdrive/dmoz/dmoz_toplevel_lang.csv")
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

		// var refined

		cats := strings.Split(entry[3], "/")
		if len(cats) < 2 {
			continue
		}

		fullPath := entry[3]
		var parentPath string
		switch cats[1] {
		case "Regional":
			if len(cats) < 5 {
				continue
			}
			entry[3] = cats[4]
			parentPath = strings.Join(cats[:4], "/")
		case "World":
			if len(cats) < 4 {
				continue
			}
			entry[3] = cats[3]
			parentPath = strings.Join(cats[:3], "/")
		default:
			parentPath = cats[0]
			entry[3] = cats[1]
		}

		info := whatlanggo.Detect(entry[2])
		entry = append(entry, info.Lang.String())
		entry = append(entry, whatlanggo.Scripts[info.Script])
		entry = append(entry, info.Lang.Iso6391())
		entry = append(entry, info.Lang.Iso6391())
		entry = append(entry, fmt.Sprintf(strconv.FormatFloat(info.Confidence, 'f', -1, 64)))
		entry = append(entry, fullPath)
		entry = append(entry, parentPath)

		csvTopLevel.Write(entry)
		csvTopLevel.Flush()
	}

}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
