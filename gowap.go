package main

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"
	"github.com/lucmichalski/dmoz-utils/pkg/gowap"
)

func main() {
	url := "https://dgraph.io/blog/"
	wapp, err := gowap.Init("./apps.json", false)
	if err != nil {
		log.Errorln(err)
	}
	res, err := wapp.Analyze(url)
	if err != nil {
		log.Errorln(err)
	}
	prettyJSON, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		log.Errorln(err)
	}
	log.Infof("[*] Result for %s:\n%s", url, string(prettyJSON))
}
