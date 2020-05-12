package main

import (
	"io/ioutil"
	"log"
	"time"

	"github.com/beevik/etree"
)

type Dmoz struct {
	Cateid string   `json:"cateid"`
	Topic  string   `json:"topic"`
	Link   []string `json:"link"`
}

func main() {
	var dmozs []Dmoz
	starttime := time.Now()
	dat, err := ioutil.ReadFile("./shared/dataset/content.rdf.u8")
	if err != nil {
		panic(err)
	}

	doc := etree.NewDocument()
	err = doc.ReadFromBytes(dat)

	if err != nil {
		log.Println(err, string(dat))
	}

	root := doc.SelectElement("RDF")

	for _, entry := range root.SelectElements("Topic") {
		d := Dmoz{}

		//catid
		catId := entry.SelectElement("catid")
		if catId != nil {
			log.Println("catid is:", catId.Text())
			d.Cateid = catId.Text()
		} else {
			continue
		}

		topic := entry.SelectAttr("r:id").Value
		log.Println("topic is:", topic)
		d.Topic = topic

		//link1
		var linkarr []string
		link1 := entry.SelectElement("link1")
		if link1 != nil {
			url := link1.SelectAttr("r:resource").Value
			log.Println("url is:", url)
			linkarr = append(linkarr, url)
		}
		//link
		links := entry.SelectElements("link")
		if links != nil {
			for _, url := range links {
				urltxt := url.SelectAttr("r:resource").Value
				log.Println("url is:", urltxt)
				linkarr = append(linkarr, urltxt)
			}
			d.Link = linkarr
		}
		dmozs = append(dmozs, d)
	}

	log.Println("Finish Parsing RDF xml, time period:", time.Now().Sub(starttime))

	//elk := "http://localhost:9200"
	////setup elastic client for bulk update
	//client, err := elastic.NewClient(
	//elastic.SetSniff(false),
	//elastic.SetURL(elk),
	//)
	//if err != nil {
	//log.Println("Elasticsearch Server is dead.")
	//panic(err)
	//}

	////initialize bulk update service
	//bulkUpdateRequest := client.Bulk()
	//cnt := 0
	//for _, v := range dmozs {

	//updateRequest := elastic.NewBulkUpdateRequest().
	//Index("dmoz").
	//Type("content").
	//Id(v.Cateid).
	//Doc(v).
	//DocAsUpsert(true).
	//RetryOnConflict(3)

	//bulkUpdateRequest = bulkUpdateRequest.Add(updateRequest)

	//cnt++
	//if t := cnt % 1000; t == 0 {
	//log.Println("")
	//log.Println("Number of actions:", bulkUpdateRequest.NumberOfActions())
	//log.Println("")

	////execute Bulk Update
	//_, brErr := bulkUpdateRequest.Do()
	//time.Sleep(1 * time.Second)
	//if brErr != nil {
	//log.Println("Bulk Update to ELK failed.", brErr)
	//} else {
	//bulkUpdateRequest = client.Bulk()
	//}
	//}
	//}

	//log.Println("")
	//log.Println("Number of actions:", bulkUpdateRequest.NumberOfActions())
	//log.Println("")

	////execute Bulk Update to the last data
	//_, brErr := bulkUpdateRequest.Do()
	//if brErr != nil {
	//log.Println("Bulk Update to ELK failed.", brErr)
	//}

}
