package main

import (
	"encoding/csv"
	"fmt"
	// "io"
	"math/rand"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	// "github.com/k0kubun/pp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nozzle/throttler"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/tebeka/selenium"
	"github.com/tebeka/selenium/chrome"
	slog "github.com/tebeka/selenium/log"
)

var (
	DB           *gorm.DB
	isUnescape   bool
	isOffset     int
	isLimit      int
	isVerbose    bool
	isHelp       bool
	parallelJobs int
)

func main() {
	pflag.IntVarP(&isOffset, "offset", "", 0, "offset x times the limit")
	pflag.IntVarP(&isLimit, "limit", "", 500000, "limit the number of results returned.")
	pflag.BoolVarP(&isUnescape, "unescape", "u", false, "unescape path characters")
	pflag.BoolVarP(&isVerbose, "verbose", "v", false, "verbose mode.")
	pflag.BoolVarP(&isHelp, "help", "h", false, "help info.")
	pflag.Parse()
	if isHelp {
		pflag.PrintDefaults()
		os.Exit(1)
	}
	// url := "https://www.bloomberg.com/news/articles/2020-03-11/augmented-reality-startup-magic-leap-is-said-to-explore-a-sale"
	// url := "https://www.leboncoin.fr/voitures/1781740745.htm/"
	// url := "https://www.alexa.com/topsites/category/Top/Health/Medicine"

	// init database
	var err error
	DB, err = gorm.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=True&loc=Local", "root", "megaweb", "localhost", "3308", "dataset_dmoz"))
	if err != nil {
		log.Fatal(err)
	}

	DB.Set("gorm:table_options", "ENGINE=InnoDB CHARSET=utf8mb4")
	DB.AutoMigrate(&AlexaWebsite{})
	DB.AutoMigrate(&Category{})
	DB.AutoMigrate(&Rss{})
	DB.AutoMigrate(&Rank{})
	DB.AutoMigrate(&Sitemap{})
	validations.RegisterCallbacks(DB)

	// fix path's escaping
	if isUnescape {
		scanPaths(DB)
		os.Exit(0)
	}

	caps := selenium.Capabilities{"browserName": "chrome"}
	chromeCaps := chrome.Capabilities{
		Args: []string{
			"--headless",
			"--no-sandbox",
			"--start-maximized",
			"--window-size=1024,768",
			"--disable-crash-reporter",
			"--hide-scrollbars",
			"--disable-gpu",
			"--disable-setuid-sandbox",
			"--disable-infobars",
			"--window-position=0,0",
			"--ignore-certifcate-errors",
			"--ignore-certifcate-errors-spki-list",
			"--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/604.4.7 (KHTML, like Gecko) Version/11.0.2 Safari/604.4.7",
			// "--proxy-server=http://localhost:1080", // 1080  // 5566 // 8119
			// "--host-resolver-rules=\"MAP * 0.0.0.0 , EXCLUDE localhost\"",
		},
	}
	caps.AddChrome(chromeCaps)

	caps.SetLogLevel(slog.Server, slog.Off)
	caps.SetLogLevel(slog.Browser, slog.Off)
	caps.SetLogLevel(slog.Client, slog.Off)
	caps.SetLogLevel(slog.Driver, slog.Off)
	caps.SetLogLevel(slog.Performance, slog.Off)
	caps.SetLogLevel(slog.Profiler, slog.Off)

	wd, err := selenium.NewRemote(caps, fmt.Sprintf("http://localhost:%d/wd/hub", 4444))
	if err != nil {
		log.Fatal(err)
	}
	defer wd.Quit()

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
	rand.Seed(time.Now().UnixNano())
	shuffle(data)

	/*
		for {
			loc, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				if perr, ok := err.(*csv.ParseError); ok && perr.Err == csv.ErrFieldCount {
					continue
				}
				log.Fatal(err)
			}
	*/

	for _, loc := range data {

		if !strings.Contains(loc[0], "topsites/category") {
			continue
		}

		// get the category
		path := strings.Replace(loc[0], "https://www.alexa.com/topsites/category/", "", -1)
		decodedPath, err := url.QueryUnescape(path)
		if err != nil {
			log.Fatal(err)
			return
		}

		// check if exists
		var categoryExists AlexaWebsite
		if !DB.Where("path = ?", decodedPath).First(&categoryExists).RecordNotFound() {
			fmt.Println("Skipping path:", decodedPath)
			continue
		}

		fmt.Println("processing: ", decodedPath)
		err = wd.Get(loc[0])
		if err != nil {
			log.Fatal(err)
		}

		// display source
		src, err := wd.PageSource()
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("source", src)

		elems, err := wd.FindElements(selenium.ByCSSSelector, ".tr.site-listing")
		if err != nil {
			panic(err)
		}

		for _, e := range elems {
			// Get a reference to the text box containing code.
			/*
				elem, err := e.FindElement(selenium.ByCSSSelector, "div.td:first-child")
				if err != nil {
					panic(err)
				}
				output, err := elem.Text()
				if err != nil {
					panic(err)
				}
				log.Println("Number: ", output)
			*/

			elem, err := e.FindElement(selenium.ByCSSSelector, "div.td.DescriptionCell")
			if err != nil {
				panic(err)
			}
			output, err := elem.Text()
			if err != nil {
				panic(err)
			}
			log.Println("Site: ", output)

			entry := &AlexaWebsite{
				Link: output,
				Path: decodedPath,
			}

			err = createOrUpdateWebsite(DB, entry)
			if err != nil {
				log.Warnln(err)
			}

			/*
				elem, err = e.FindElement(selenium.ByCSSSelector, "div.td:nth-child(3)")
				if err != nil {
					panic(err)
				}
				output, err = elem.Text()
				if err != nil {
					panic(err)
				}
				log.Println("DailyTimeOnSite: ", output)

				elem, err = e.FindElement(selenium.ByCSSSelector, "div.td:nth-child(4)")
				if err != nil {
					panic(err)
				}
				output, err = elem.Text()
				if err != nil {
					panic(err)
				}
				log.Println("DailyPageviewsPerVisitor: ", output)

				elem, err = e.FindElement(selenium.ByCSSSelector, "div.td:nth-child(5)")
				if err != nil {
					panic(err)
				}
				output, err = elem.Text()
				if err != nil {
					panic(err)
				}
				log.Println("PercentOfTrafficFromSearch: ", output)

				elem, err = e.FindElement(selenium.ByCSSSelector, "div.td:nth-child(6)")
				if err != nil {
					panic(err)
				}
				output, err = elem.Text()
				if err != nil {
					panic(err)
				}
				log.Println("TotalSitesLinkingIn: ", output)
			*/
		}
	}
}

func scanPaths(db *gorm.DB) {
	offset := isOffset * isLimit

	type result struct {
		Link string
		Path string
	}

	var results []result
	query := fmt.Sprintf("select link, path FROM alexa_websites ORDER BY RAND() LIMIT %d,%d", offset, isLimit)
	fmt.Println("query:", query)

	t := throttler.New(12, 100000000)

	DB.Raw(query).Scan(&results)
	for _, r := range results {
		// pp.Println(r)
		go func(entry result) error {
			defer t.Done(nil)
			fmt.Println("entry.Link:", entry.Link, "entry.Path:", entry.Path)
			website := &AlexaWebsite{}
			if !DB.Where("link = ?", entry.Link).First(&website).RecordNotFound() {
				decodedPath, err := url.QueryUnescape(entry.Path)
				if err != nil {
					log.Fatal(err)
					return err
				}
				website.Path = decodedPath
				// pp.Println(website)
				// save website
				if err := DB.Save(website).Error; err != nil {
					return err
				}
			}
			return nil
		}(r)
		t.Throttle()
	}

	// throttler errors iteration
	if t.Err() != nil {
		// Loop through the errors to see the details
		for i, err := range t.Errs() {
			log.Printf("error #%d: %s", i, err)
		}
		log.Fatal(t.Err())
	}

}

func createOrUpdateWebsite(db *gorm.DB, website *AlexaWebsite) error {
	var existingWebsite AlexaWebsite
	if db.Where("link = ?", website.Link).First(&existingWebsite).RecordNotFound() {
		return db.Create(website).Error
	}
	website.ID = existingWebsite.ID
	website.CreatedAt = existingWebsite.CreatedAt
	return db.Save(website).Error
}

type AlexaWebsite struct {
	gorm.Model
	Link           string   `gorm:"size:255;unique"`
	Alive          bool     `gorm:"index:alive"`
	StatusCode     int      `gorm:"index:status_code"`
	Name           string   `gorm:"index:name; type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"`
	Path           string   `gorm:"index:path; type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci"`
	Title          string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	Description    string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	CategoryID     uint     `l10n:"sync"`
	Category       Category `l10n:"sync"`
	Wap            string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	Analyzed       int      `gorm:"type:tinyint" sql:"type:tinyint`
	TextExtract    string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	ArticleText    string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	RobotsTxt      string   `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Host           string
	Scheme         string
	Domain         string
	Tld            string
	Language       string
	LangConfidence float64
	Ranking        Rank
	Rss            []Rss
	Sitemaps       []Sitemap
}

type Sitemap struct {
	gorm.Model
	Href      string `sql:"type:longtext"`
	Index     bool
	Gziped    bool
	WebsiteID uint
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

func shuffle(slice interface{}) {
	rv := reflect.ValueOf(slice)
	swap := reflect.Swapper(slice)
	length := rv.Len()
	for i := length - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		swap(i, j)
	}
}
