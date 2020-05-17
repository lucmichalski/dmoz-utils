package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/beevik/etree"
	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/proxy"
	"github.com/gocolly/colly/v2/queue"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nozzle/throttler"
	"github.com/qor/admin"
	"github.com/qor/assetfs"
	"github.com/qor/qor/utils"
	"github.com/qor/validations"
	"github.com/spf13/pflag"

	ccsv "github.com/lucmichalski/dmoz-utils/pkg/csv"
)

var (
	isHelp       bool
	isVerbose    bool
	isAdmin      bool
	isDataset    bool
	isScanFeeds  bool
	isImport     bool
	isDump       bool
	parallelJobs int
	queueMaxSize = 100000000
	cachePath    = "./data/cache"
	DB           *gorm.DB
)

func main() {
	pflag.IntVarP(&parallelJobs, "parallel-jobs", "j", 3, "parallel jobs.")
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
		DB, err = gorm.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?parseTime=True", "root", "megaweb", "localhost", "3308", "dataset_dmoz"))
		if err != nil {
			log.Fatal(err)
		}
		DB.AutoMigrate(&Website{}, &Category{}, &Rss{}, &Rank{})
		validations.RegisterCallbacks(DB)
	}

	// Initialize Admin
	if isAdmin {

		// Initialize AssetFS
		AssetFS := assetfs.AssetFS().NameSpace("admin")

		// Register custom paths to manually saved views
		AssetFS.RegisterPath(filepath.Join(utils.AppRoot, "./templates/qor/admin/views"))
		AssetFS.RegisterPath(filepath.Join(utils.AppRoot, "./templates/qor/media/views"))

		// Initialize Admin
		Admin := admin.New(&admin.AdminConfig{
			SiteName: "DMOZ Dataset",
			DB:       DB,
			AssetFS:  AssetFS,
		})

		// Allow to use Admin to manage User, Product
		website := Admin.AddResource(&Website{}, &admin.Config{Menu: []string{"Website Management"}, Priority: -1})
		website.IndexAttrs("ID", "Link", "Path")

		category := Admin.AddResource(&Category{}, &admin.Config{Menu: []string{"Website Management"}, Priority: -3})
		category.Meta(&admin.Meta{Name: "Categories", Type: "select_many"})

		// initalize an HTTP request multiplexer
		mux := http.NewServeMux()

		// Mount admin interface to mux
		Admin.MountTo("/admin", mux)

		router := gin.Default()
		admin := router.Group("/admin", gin.BasicAuth(gin.Accounts{"dmoz": "dmoz"}))
		{
			admin.Any("/*resources", gin.WrapH(mux))
		}

		router.Static("/public", "./public")

		fmt.Println("Listening on: 9001")
		s := &http.Server{
			Addr:           ":9001",
			Handler:        router,
			ReadTimeout:    10 * time.Second,
			WriteTimeout:   10 * time.Second,
			MaxHeaderBytes: 1 << 20,
		}
		s.ListenAndServe()
		os.Exit(1)

	}

	// import data
	// importRdf("./shared/dataset/content.rdf.u8", DB)
	// scanFeeds(DB)

	loadData("./dmoz_dataset.csv", DB)

}

// LOAD DATA INFILE '/root/dmoz_dataset.csv' INTO TABLE websites FIELDS TERMINATED BY '\t' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (link,path);
func loadData(csvFile string, DB *gorm.DB) {
	fmt.Println("loading data from file...")

	mysql.RegisterLocalFile(csvFile)
	query := `LOAD DATA LOCAL INFILE '` + csvFile + `' INTO TABLE websites CHARACTER SET 'utf8mb4' FIELDS TERMINATED BY '\t' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES (link,path) SET created_at = NOW(), updated_at = NOW();`

	fmt.Println(query)
	err := DB.Exec(query).Error
	if err != nil {
		log.Fatal(err)
	}
}

func importRdf(rdfFile string, DB *gorm.DB) {

	// create dump file
	csvDmoz, err := ccsv.NewCsvWriter("dmoz_dataset.csv")
	if err != nil {
		panic("Could not open `csvSitemap.csv` for writing")
	}

	// Flush pending writes and close file upon exit of Sitemap()
	defer csvDmoz.Close()

	csvDmoz.Write([]string{"link", "topic"})
	csvDmoz.Flush()

	starttime := time.Now()
	dat, err := ioutil.ReadFile(rdfFile)
	if err != nil {
		panic(err)
	}

	doc := etree.NewDocument()
	err = doc.ReadFromBytes(dat)

	if err != nil {
		log.Println(err, string(dat))
	}

	root := doc.SelectElement("RDF")

	t := throttler.New(42, 100000000)

	for _, entry := range root.SelectElements("Topic") {

		go func(entry *etree.Element) error {
			defer t.Done(nil)

			c := &Category{}
			//catid
			catId := entry.SelectElement("catid")
			if catId != nil {
				log.Println("catid is:", catId.Text())
				c.Code = catId.Text()
			} else {
				// continue
				return nil
			}

			topic := entry.SelectAttr("r:id").Value
			log.Println("topic is:", topic)
			if topic == "" {
				topic = "Root"
			}
			// topicParts := strings.Split(strings.Replace(topic, "_", " ", -1), "/")
			/*
				var err error
				var cc *Category
				for _, topicPart := range topicParts {
					c.Name = topicPart
					cc, err = createOrUpdateCategory(DB, c)
					checkErr(err)
				}
			*/

			c.Name = topic

			var err error
			cc := &Category{}
			if !isDump {
				cc, err = createOrUpdateCategory(DB, c)
				if err != nil {
					return err
				}
			}

			//link1
			link1 := entry.SelectElement("link1")
			if link1 != nil {
				url := link1.SelectAttr("r:resource").Value
				log.Println("url is:", url)
				website := &Website{}
				website.Category = *cc
				website.Link = url
				website.Path = topic

				if !isDump {
					_, err := createOrUpdateWebsite(DB, website)
					if err != nil {
						return err
					}
				} else {
					csvDmoz.Write([]string{website.Link, website.Path})
					csvDmoz.Flush()
				}
			}

			//link
			links := entry.SelectElements("link")
			if links != nil {
				for _, url := range links {
					urltxt := url.SelectAttr("r:resource").Value
					log.Println("url is:", urltxt)
					website := &Website{}
					website.Category = *cc
					website.Link = urltxt
					website.Path = topic
					if !isDump {
						_, err := createOrUpdateWebsite(DB, website)
						if err != nil {
							return err
						}
					} else {
						csvDmoz.Write([]string{website.Link, website.Path})
						csvDmoz.Flush()
					}
				}
			}
			return nil
		}(entry)

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

	log.Println("Finish Parsing RDF xml, time period:", time.Now().Sub(starttime))
}

func scanFeeds(DB *gorm.DB) {

	// Instantiate default collector
	c := colly.NewCollector(
		colly.CacheDir(cachePath),
	)

	rp, err := proxy.RoundRobinProxySwitcher("socks5://127.0.0.1:5566", "socks5://127.0.0.1:8119")
	if err != nil {
		log.Fatal(err)
	}
	c.SetProxyFunc(rp)

	// create a request queue with 1 consumer thread
	q, _ := queue.New(
		parallelJobs, // Number of consumer threads set to 1 to avoid dead lock on database
		&queue.InMemoryQueueStorage{
			MaxSize: queueMaxSize,
		}, // Use default queue storage
	)

	c.OnError(func(r *colly.Response, err error) {
		fmt.Println("error:", err, r.Request.URL, r.StatusCode)
		q.AddURL(r.Request.URL.String())
	})

	c.OnResponse(func(r *colly.Response) {
		time.Sleep(1 * time.Second)
		if isVerbose {
			fmt.Println("OnResponse from", r.Ctx.Get("url"))
		}
	})

	// Before making a request print "Visiting ..."
	c.OnRequest(func(r *colly.Request) {
		//if isVerbose {
		fmt.Println("Visiting", r.URL.String())
		//}
		r.Ctx.Put("url", r.URL.String())
	})

	c.OnHTML(`html`, func(e *colly.HTMLElement) {
		var websiteExists Website
		if !DB.Where("link = ?", e.Request.Ctx.Get("url")).First(&websiteExists).RecordNotFound() {
			fmt.Printf("skipping url=%s as already exists\n", e.Request.Ctx.Get("url"))
			return
		}

		e.ForEach(`type="application/rss+xml"`, func(_ int, el *colly.HTMLElement) {
			rss := el.Attr("href")
			if rss != "" {
				websiteExists.Rss = append(websiteExists.Rss, Rss{Href: rss})
			}
		})

		// Update entry
		if err := DB.Save(websiteExists).Error; err != nil {
			log.Fatalln("could not update entry: ", err)
		}

	})

	type res struct {
		Link string
	}

	// Load list of websites.
	var results []res
	DB.Raw("select link FROM websites").Scan(&results)

	for _, result := range results {
		q.AddURL(result.Link)
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
	Name        string   `gorm:"index:name"`
	Path        string   `gorm:"index:path"`
	Description string   `sql:"type:text"`
	CategoryID  uint     `l10n:"sync"`
	Category    Category `l10n:"sync"`
	Ranking     Rank
	Rss         []Rss
}

type Rss struct {
	gorm.Model
	Href string
}

type Rank struct {
	gorm.Model
	Alexa    int
	Quancast int
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
