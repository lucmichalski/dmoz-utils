package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	// "regexp"
	"strings"
	"time"

	"github.com/beevik/etree"
	"github.com/jinzhu/gorm"
	// _ "github.com/jinzhu/gorm/dialects/mysql"
	// "github.com/k0kubun/pp"
	_ "github.com/mattn/go-sqlite3"
	// "github.com/nozzle/throttler"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/qor/media"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"golang.org/x/net/proxy"

	ccsv "github.com/lucmichalski/dmoz-utils/pkg/csv"
)

var (
	// store          *badger.DB
	isHelp       bool
	isVerbose    bool
	isAdmin      bool
	isDataset    bool
	isProxy      bool
	parallelJobs int
	queueMaxSize = 100000000
	cachePath    = "./data/cache"
	// storagePath    = "./data/badger"
	sitemapRootURL = "https://s3.amazonaws.com/com.alexa.sitemap/sitemap_index.xml"
	//pageTestURL = "https://www.alexa.com/topsites/category/Top/Health/Medicine"
)

const (
	torProxyAddress   = "socks5://51.91.21.67:5566"
	torPrivoxyAddress = "socks5://51.91.21.67:8119"
)

type website struct {
	gorm.Model
	Site                 string `gorm:"size:255;unique"`
	DetectLang           string
	DetectLangConfidence float64
	Categories           []category `gorm:"many2many:feed_categories;"`
	CategoryPath         string
}

type category struct {
	gorm.Model
	Name string
}

func main() {
	pflag.IntVarP(&parallelJobs, "parallel-jobs", "j", 3, "parallel jobs.")
	pflag.BoolVarP(&isDataset, "dataset", "d", false, "dump dataset.")
	pflag.BoolVarP(&isAdmin, "admin", "a", false, "launch web admin.")
	pflag.BoolVarP(&isVerbose, "verbose", "v", false, "verbose mode.")
	pflag.BoolVarP(&isHelp, "help", "h", false, "help info.")
	pflag.Parse()
	if isHelp {
		pflag.PrintDefaults()
		os.Exit(1)
	}

	// pp.Println(fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4,utf8&parseTime=True", os.Getenv("ND_MYSQL_USER"), os.Getenv("ND_MYSQL_PASSWORD"), os.Getenv("ND_MYSQL_HOST"), os.Getenv("ND_MYSQL_PORT"), "dataset_news"))

	// Instanciate the mysql client
	DB, err := gorm.Open("sqlite3", "similarweb.db")
	// DB, err := gorm.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4,utf8&parseTime=True", os.Getenv("ND_MYSQL_USER"), os.Getenv("ND_MYSQL_PASSWORD"), os.Getenv("ND_MYSQL_HOST"), os.Getenv("ND_MYSQL_PORT"), "dataset_news"))
	if err != nil {
		log.Fatal(err)
	}
	defer DB.Close()

	// callback for images and validation
	validations.RegisterCallbacks(DB)
	media.RegisterCallbacks(DB)

	DB.AutoMigrate(&category{})
	DB.AutoMigrate(&website{})

	linksSitemap, err := ccsv.NewCsvWriter("similarweb_links.csv")
	if err != nil {
		panic("Could not open `similarweb_links.csv` for writing")
	}
	defer linksSitemap.Close()

	// new concurrent map
	m := cmap.New()

	similarWebSitemaps := []string{
		"https://www.similarweb.com/sitemaps/website/website-index.xml.gz",
		"https://www.similarweb.com/sitemaps/website/top-website-index.xml.gz",
		"https://www.similarweb.com/sitemaps/app/top-app-index.xml.gz",
		"https://www.similarweb.com/sitemaps/app/app-index.xml.gz",
		// "https://www.similarweb.com/corp/sitemap_index.xml",
	}

	// for _, sitemaps := range similarWebSitemaps {

	// Start scraping on https://www.autosphere.fr
	// log.Infoln("extractSitemapIndex...")
	// sitemaps, err := extractSitemapIndex(sitemapRootURL)
	// if err != nil {
	// 	log.Fatal("ExtractSitemapIndex:", err)
	// }

	// shuffle(sitemaps)
	for _, sitemap := range similarWebSitemaps {
		log.Infoln("processing ", sitemap)
		if strings.Contains(sitemap, ".gz") {
			log.Infoln("extract sitemap gz compressed...")
			// rename url parts
			locs, err := extractSitemapGZ(sitemap)
			if err != nil {
				log.Warnln("ExtractSitemapGZ", err)
			}
			shuffle(locs)
			for _, loc := range locs {
				// if strings.Contains(loc, "topsites") {
				fmt.Println("loc:", loc)
				linksSitemap.Write([]string{loc})
				linksSitemap.Flush()
				// }
			}
		} else {
			fmt.Println("sitemap:", sitemap)
		}
	}
	//}

	log.Println("Collected cmap: ", m.Count(), "users")
	os.Exit(1)

	/*
		time.Sleep(10 * time.Second)

		t := throttler.New(6, m.Count())

		m.IterCb(func(key string, v interface{}) {

			go func(key string) error {
				// Let Throttler know when the goroutine completes
				// so it can dispatch another worker
				defer t.Done(nil)

				pp.Println("new key: ", key)

				time.Sleep(1 * time.Second)

				return nil
			}(key)
			t.Throttle()
		})

		// throttler errors iteration
		if t.Err() != nil {
			// Loop through the errors to see the details
			for i, err := range t.Errs() {
				log.Warnf("error #%d: %s", i, err)
			}
			// log.Fatal(t.Err())
		}
	*/

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

func extractSitemapIndex(rawUrl string) ([]string, error) {
	client := &http.Client{
		Timeout: 40 * time.Second,
	}

	tbProxyURL, err := url.Parse(torProxyAddress)
	if err != nil {
		return nil, err
	}

	tbDialer, err := proxy.FromURL(tbProxyURL, proxy.Direct)
	if err != nil {
		return nil, err
	}
	tbTransport := &http.Transport{
		Dial: tbDialer.Dial,
	}
	client.Transport = tbTransport

	request, err := http.NewRequest("GET", rawUrl, nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer response.Body.Close()

	doc := etree.NewDocument()
	if _, err := doc.ReadFrom(response.Body); err != nil {
		return nil, err
	}
	var urls []string
	index := doc.SelectElement("sitemapindex")
	sitemaps := index.SelectElements("sitemap")
	for _, sitemap := range sitemaps {
		loc := sitemap.SelectElement("loc")
		log.Infoln("loc:", loc.Text())
		urls = append(urls, loc.Text())
	}
	return urls, nil
}

func extractSitemapGZ(rawUrl string) ([]string, error) {
	client := &http.Client{
		Timeout: 40 * time.Second,
	}

	tbProxyURL, err := url.Parse(torProxyAddress)
	if err != nil {
		return nil, err
	}

	tbDialer, err := proxy.FromURL(tbProxyURL, proxy.Direct)
	if err != nil {
		return nil, err
	}
	tbTransport := &http.Transport{
		Dial:               tbDialer.Dial,
		DisableCompression: true,
	}
	client.Transport = tbTransport

	request, err := http.NewRequest("GET", rawUrl, nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer response.Body.Close()

	var reader io.ReadCloser
	reader, err = gzip.NewReader(response.Body)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer reader.Close()

	/*
		s, err := ioutil.ReadAll(reader)
		if err != nil {
			panic(err)
		}

		fmt.Println("decompressed:\t", string(s))
	*/

	doc := etree.NewDocument()
	if _, err := doc.ReadFrom(reader); err != nil {
		panic(err)
	}
	var urls []string
	urlset := doc.SelectElement("urlset")
	entries := urlset.SelectElements("url")
	for _, entry := range entries {
		loc := entry.SelectElement("loc")
		log.Infoln("loc:", loc.Text())
		urls = append(urls, loc.Text())
	}
	return urls, err
}

func readGzip(content []byte) error {
	var buf *bytes.Buffer = bytes.NewBuffer(content)

	gRead, err := zlib.NewReader(buf)
	if err != nil {
		return err
	}

	if t, err := io.Copy(os.Stdout, gRead); err != nil {
		fmt.Println(t)
		return err
	}

	if err := gRead.Close(); err != nil {
		return err
	}
	return nil
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		elements[v] = strings.ToLower(elements[v])
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func ensureDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		os.MkdirAll(path, os.FileMode(0755))
	} else {
		return err
	}
	d.Close()
	return nil
}
