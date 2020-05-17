package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	// "github.com/emiruz/textextract"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/proxy"
	"github.com/gocolly/colly/v2/queue"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nozzle/throttler"
	"github.com/qor/validations"
	"github.com/samclarke/robotstxt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/lucmichalski/dmoz-utils/pkg/articletext"
	"github.com/lucmichalski/dmoz-utils/pkg/robotstxt"
)

var (
	isHelp       bool
	isVerbose    bool
	isAdmin      bool
	isDataset    bool
	isImport     bool
	isDump       bool
	isLoadData   bool
	isScanFeeds  bool
	isImportRDF  bool
	isTorProxy   bool
	isOffset     int
	isLimit      int
	parallelJobs int
	queueMaxSize = 100000000
	cachePath    = "./data/cache"
	DB           *gorm.DB
)

func main() {
	pflag.IntVarP(&isOffset, "offset", "", 0, "offset x times the limit")
	pflag.IntVarP(&isLimit, "limit", "", 500000, "limit the number of results returned.")
	pflag.IntVarP(&parallelJobs, "parallel-jobs", "j", 64, "parallel jobs.")
	pflag.BoolVarP(&isImportRDF, "rdf", "r", false, "import rdf file 'content.rdf.u8'.")
	pflag.BoolVarP(&isLoadData, "load", "l", false, "load data into file.")
	pflag.BoolVarP(&isTorProxy, "proxy", "x", false, "use tor proxy.")
	pflag.BoolVarP(&isDump, "dump", "p", false, "create csv dump.")
	pflag.BoolVarP(&isDataset, "dataset", "d", false, "generate dataset from db.")
	pflag.BoolVarP(&isScanFeeds, "scan", "s", false, "scan for rss feeds.")
	pflag.BoolVarP(&isImport, "import", "i", false, "import rdf file to database.")
	pflag.BoolVarP(&isAdmin, "admin", "a", false, "launch web admin.")
	pflag.BoolVarP(&isVerbose, "verbose", "v", false, "verbose mode.")
	pflag.BoolVarP(&isHelp, "help", "h", false, "help info.")
	pflag.Parse()
	if isHelp {
		pflag.PrintDefaults()
		os.Exit(1)
	}

	var err error
	if !isDump {
		DB, err = gorm.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=True&loc=Local", "root", "megaweb", "localhost", "3308", "dataset_dmoz"))
		if err != nil {
			log.Fatal(err)
		}

		DB.Set("gorm:table_options", "ENGINE=InnoDB CHARSET=utf8mb4")
		DB.AutoMigrate(&Website{})
		DB.AutoMigrate(&Category{})
		DB.AutoMigrate(&Rss{})
		DB.AutoMigrate(&Rank{})
		validations.RegisterCallbacks(DB)
	}

	// Instantiate default collector
	c := colly.NewCollector(
	// colly.CacheDir(cachePath),
	)

	if isTorProxy {
		rp, err := proxy.RoundRobinProxySwitcher("socks5://127.0.0.1:5566", "socks5://127.0.0.1:8119")
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

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("error:", err, r.Request.URL, r.StatusCode)
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

	c.OnHTML(`html`, func(e *colly.HTMLElement) {
		// get text summary
		textextract.MinScore = 5 // the default is 5.
		extractedText, err := textextract.ExtractFromHtml(e.Text)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(extractedText)
	})

	type res struct {
		Link string
	}

	// Load list of websites.
	var results []res
	offset := isOffset * isLimit

	query := fmt.Sprintf("select link FROM websites WHERE analyzed=1 ORDER BY RAND() LIMIT %d,%d", offset, isLimit)
	fmt.Println("query:", query)

	DB.Raw(query).Scan(&results)
	for _, result := range results {
		if strings.HasPrefix(result.Link, "http") {
			fmt.Println("enqueuing", result.Link)
			q.AddURL(result.Link)
		}
	}

	// Consume URLs
	q.Run(c)

}

func createOrUpdateWebsite(db *gorm.DB, website *Website) (*Website, error) {
	var existingWebsite Website
	if db.Where("link = ?", website.Link).First(&existingWebsite).RecordNotFound() {
		err := db.Create(website).Error
		return website, err
	}
	website.ID = existingWebsite.ID
	website.CreatedAt = existingWebsite.CreatedAt
	return website, nil
}

func createOrUpdateCategory(db *gorm.DB, cat *Category) (*Category, error) {
	var existingCategory Category
	if db.Where("name = ?", cat.Name).First(&existingCategory).RecordNotFound() {
		err := db.Create(cat).Error
		return cat, err
	}
	cat.ID = existingCategory.ID
	cat.CreatedAt = existingCategory.CreatedAt
	return cat, nil
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type Website struct {
	gorm.Model
	Link        string   `gorm:"size:255;unique"`
	Alive       bool     `gorm:"index:alive"`
	StatusCode  int      `gorm:"index:status_code"`
	Name        string   `gorm:"index:name; type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"`
	Path        string   `gorm:"index:path; type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"`
	Title       string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	Description string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	CategoryID  uint     `l10n:"sync"`
	Category    Category `l10n:"sync"`
	Wap         string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	Analyzed    int      `gorm:"type:tinyint" sql:"type:tinyint`
	Ranking     Rank
	Rss         []Rss
}

type Rss struct {
	gorm.Model
	Href               string `sql:"type:longtext"`
	Language           string
	LanguageConfidence float64
	WebsiteID          uint
}

type Rank struct {
	gorm.Model
	Alexa    int
	Quancast int
	Majestic int
}

type Category struct {
	gorm.Model
	Name       string `gorm:"index:name"`
	Code       string `gorm:"index:code"`
	Categories []Category
	CategoryID uint `gorm:"index:category_id"`
}

func (category Category) Validate(db *gorm.DB) {
	if strings.TrimSpace(category.Name) == "" {
		db.AddError(validations.NewError(category, "Name", "Name can not be empty"))
	}
}

func (category Category) DefaultPath() string {
	if len(category.Code) > 0 {
		return fmt.Sprintf("/category/%s", category.Code)
	}
	return "/"
}
