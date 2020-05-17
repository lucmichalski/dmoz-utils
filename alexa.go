package main

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	// "io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"

	// "github.com/abadojack/whatlanggo"
	"github.com/beevik/etree"
	"github.com/gin-gonic/gin"
	"github.com/gocolly/colly/v2"
	cproxy "github.com/gocolly/colly/v2/proxy"
	"github.com/gocolly/colly/v2/queue"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/k0kubun/pp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nozzle/throttler"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/qor/admin"
	"github.com/qor/assetfs"
	"github.com/qor/media"
	"github.com/qor/qor/utils"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"golang.org/x/net/proxy"

	// padmin "github.com/lucmichalski/dmoz-utils/pkg/admin"
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
	// sitemapRootURL = "https://s3.amazonaws.com/com.alexa.sitemap/sitemap_index.xml"
	pageTestURL = "https://www.alexa.com/topsites/category/Top/Health/Medicine"
)

const (
	torProxyAddress   = "socks5://51.91.21.67:5566"
	torPrivoxyAddress = "socks5://51.91.21.67:8119"
)

type website struct {
	gorm.Model
	Site                       string `gorm:"size:255;unique"`
	DailyTimeOnSite            string
	DailyPageviewsPerVisitor   string
	PercentOfTrafficFromSearch string
	TotalSitesLinkingIn        float64
	DetectLang                 string
	DetectLangConfidence       float64
	Categories                 []category `gorm:"many2many:feed_categories;"`
	CategoryPath               string
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
	DB, err := gorm.Open("sqlite3", "medium.db")
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

	if isDataset {

		csvDataset, err := ccsv.NewCsvWriter("alexa_dataset_refined.csv")
		if err != nil {
			panic("Could not open `alexa_dataset_refined.csv` for writing")
		}

		// Flush pending writes and close file upon exit of Sitemap()
		defer csvDataset.Close()

		csvDataset.Write([]string{"site", "category_path", "daily_time_on_site", "daily_pageviews_per_visitor", "percent_of_traffic_from_search", "total_sites_linking_in", "language", "language_confidence"})
		csvDataset.Flush()

		// Scan
		type cnt struct {
			Count int
		}

		type res struct {
			Site                       string
			DailyTimeOnSite            string
			DailyPageviewsPerVisitor   string
			PercentOfTrafficFromSearch string
			TotalSitesLinkingIn        string
			DetectLang                 string
			DetectLangConfidence       string
			CategoryPath               string
		}

		var count cnt
		DB.Raw("select count(id) as count FROM websites WHERE categories_path!=''").Scan(&count)

		// instanciate throttler
		t := throttler.New(48, count.Count)

		counter := 0
		imgCounter := 0

		var results []res
		DB.Raw("select site, category_path, daily_time_on_site, daily_pageviews_per_visitor, percent_of_traffic_from_search, total_sites_linking_in, detect_lang, detect_lang_confidence FROM websites WHERE categories_path!=''").Scan(&results)
		for _, result := range results {

			go func(r res) error {
				defer t.Done(nil)
				pp.Println(r)
				return nil
			}(result)

			t.Throttle()

		}

		fmt.Println("counter=", counter, "imgCounter=", imgCounter)

		// throttler errors iteration
		if t.Err() != nil {
			// Loop through the errors to see the details
			for i, err := range t.Errs() {
				log.Printf("error #%d: %s", i, err)
			}
			log.Fatal(t.Err())
		}

		os.Exit(0)
	}

	if isAdmin {
		// Initialize AssetFS
		AssetFS := assetfs.AssetFS().NameSpace("admin")

		// Register custom paths to manually saved views
		AssetFS.RegisterPath(filepath.Join(utils.AppRoot, "./templates/qor/admin/views"))
		AssetFS.RegisterPath(filepath.Join(utils.AppRoot, "./templates/qor/media/views"))

		// Initialize Admin
		Admin := admin.New(&admin.AdminConfig{
			SiteName: "Alexa Dataset",
			DB:       DB,
			AssetFS:  AssetFS,
		})

		// padmin.SetupDashboard(DB, Admin)

		Admin.AddResource(&category{})
		Admin.AddResource(&website{})

		// initalize an HTTP request multiplexer
		mux := http.NewServeMux()

		// Mount admin interface to mux
		Admin.MountTo("/admin", mux)

		router := gin.Default()
		admin := router.Group("/admin", gin.BasicAuth(gin.Accounts{"news": "medium"}))
		{
			admin.Any("/*resources", gin.WrapH(mux))
		}

		router.Static("/public", "./public")

		fmt.Println("Listening on: 9002")
		s := &http.Server{
			Addr:           ":9002",
			Handler:        router,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
		s.ListenAndServe()
		os.Exit(1)
	}

	csvSitemap, err := ccsv.NewCsvWriter("alexa_dataset.csv")
	if err != nil {
		panic("Could not open `alexa_dataset.csv` for writing")
	}

	// Flush pending writes and close file upon exit of Sitemap()
	defer csvSitemap.Close()

	csvSitemap.Write([]string{"site", "category_path", "daily_time_on_site", "daily_pageviews_per_visitor", "percent_of_traffic_from_search", "total_sites_linking_in", "language", "language_confidence"})
	csvSitemap.Flush()

	// new concurrent map
	m := cmap.New()

	// medium user regex pattern
	topAlexaPatternRegexp, err := regexp.Compile(`https://alexa\\.com/topsites/(.*)`)
	if err != nil {
		log.Fatal(err)
	}

	// Instantiate default collector
	c := colly.NewCollector(
		colly.UserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"),
		colly.CacheDir(cachePath),
		//colly.URLFilters(
		//	regexp.MustCompile("https://alexa\\.com/topsites/(.*)"),
		//),
	)

	if isProxy {
		rp, err := cproxy.RoundRobinProxySwitcher("socks5://127.0.0.1:1080")
		if err != nil {
			log.Fatal(err)
		}
		c.SetProxyFunc(rp)
	}

	// create a request queue with 1 consumer thread
	q, _ := queue.New(
		parallelJobs, // Number of consumer threads set to 1 to avoid dead lock on database
		&queue.InMemoryQueueStorage{
			MaxSize: queueMaxSize,
		}, // Use default queue storage
	)

	c.DisableCookies()

	// Create a callback on the XPath query searching for the URLs
	c.OnXML("//sitemap/loc", func(e *colly.XMLElement) {
		q.AddURL(e.Text)
		log.Println("//sitemap/loc", e.Text)
	})

	// https://s3.amazonaws.com/com.alexa.sitemap/sitemap.xml.gz

	// Create a callback on the XPath query searching for the URLs
	c.OnXML("//urlset/url/loc", func(e *colly.XMLElement) {
		match := topAlexaPatternRegexp.FindString(e.Text)
		if match != "" {
			log.Println("match", match)
			m.Set(match, true)
		} else {
			log.Println("skipping", e.Text)
		}
	})

	c.OnHTML(`body`, func(e *colly.HTMLElement) {
		fmt.Println("hello world !")
	})

	c.OnHTML(`div.listings.table`, func(e *colly.HTMLElement) {
		fmt.Println("hello world !")
		e.ForEach(`div.tr.site-listing td:first-child`, func(_ int, el *colly.HTMLElement) {
			log.Println("Number: ", el.Text)
		})

		e.ForEach(`div.tr.site-listing td.DescriptionCell`, func(_ int, el *colly.HTMLElement) {
			log.Println("Site", el.Text)
		})

		e.ForEach(`div.tr.site-listing td:nth-child(2)`, func(_ int, el *colly.HTMLElement) {
			log.Println("DailyTimeOnSite: ", el.Text)
		})

		e.ForEach(`div.tr.site-listing td:nth-child(3)`, func(_ int, el *colly.HTMLElement) {
			log.Println("DailyPageviewsPerVisitor: ", el.Text)
		})

		e.ForEach(`div.tr.site-listing td:nth-child(4)`, func(_ int, el *colly.HTMLElement) {
			log.Println("PercentOfTrafficFromSearch: ", el.Text)
		})

		e.ForEach(`div.tr.site-listing td:nth-child(5)`, func(_ int, el *colly.HTMLElement) {
			log.Println("TotalSitesLinkingIn: ", el.Text)
		})
		// Update entry
		//if err := DB.Save(websiteExists).Error; err != nil {
		//	log.Fatalln("could not update entry: ", err)
		//}
	})

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("error:", err, r.Request.URL, r.StatusCode)
		// q.AddURL(r.Request.URL.String())
	})

	c.OnResponse(func(r *colly.Response) {
		time.Sleep(1 * time.Second)
		if isVerbose {
			fmt.Println("OnResponse from", r.Ctx.Get("url"))
		}
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		fmt.Println("Visiting", r.URL.String())
		r.Ctx.Put("url", r.URL.String())
	})

	/*
		// Start scraping on https://www.autosphere.fr
		log.Infoln("extractSitemapIndex...")
		sitemaps, err := extractSitemapIndex(sitemapRootURL)
		if err != nil {
			log.Fatal("ExtractSitemapIndex:", err)
		}

		shuffle(sitemaps)
		for _, sitemap := range sitemaps {
			sitemap = strings.Replace(sitemap, "https://www.alexa.com/", "https://s3.amazonaws.com/com.alexa.sitemap/", -1)
			log.Infoln("processing ", sitemap)
			if strings.Contains(sitemap, ".gz") {
				log.Infoln("extract sitemap gz compressed...")
				// rename url parts
				// q.AddURL(sitemap)
				locs, err := extractSitemapGZ(sitemap)
				if err != nil {
					log.Warnln("ExtractSitemapGZ", err)
				}
				shuffle(locs)
				for _, loc := range locs {
					q.AddURL(loc)
				}
			} else {
				q.AddURL(sitemap)
			}
		}
	*/
	q.AddURL(pageTestURL)

	// Consume URLs
	q.Run(c)

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

func createOrUpdateCategory(db *gorm.DB, cat *category) (*category, error) {
	var existingCategory category
	if db.Where("name = ?", cat.Name).First(&existingCategory).RecordNotFound() {
		err := db.Create(cat).Error
		return cat, err
	}
	cat.ID = existingCategory.ID
	cat.CreatedAt = existingCategory.CreatedAt
	return cat, nil
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
