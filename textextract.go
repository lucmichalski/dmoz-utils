package main

import (
	"log"
	"fmt"
	"net/http"
	"io/ioutil"

	"github.com/emiruz/textextract"
)

func main() {

	// download page
	//url := "https://dgraph.io/blog"
	url := "http://www.kuchma.org.ua/"

   	// Get the data
    	resp, err := http.Get(url)
    	if err != nil {
		log.Fatal(err)
    	}
    	defer resp.Body.Close()

	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Trouble reading response body!")
	}

	textextract.MinScore = 5 // the default is 5.
	extractedText, err := textextract.ExtractFromHtml(string(contents))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(extractedText)
}
