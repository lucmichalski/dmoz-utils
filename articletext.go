package main

import (
	"fmt"
	"os"
	"log"

	"github.com/gelembjuk/articletext"
)

func main() {

	url := os.Args[1]
	text, err := articletext.GetArticleTextFromUrl(url)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(text)
}
