package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
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
	tld "github.com/jpillora/go-tld"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nozzle/throttler"
	"github.com/qor/admin"
	"github.com/qor/assetfs"
	"github.com/qor/qor/utils"
	"github.com/qor/validations"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"

	"github.com/lucmichalski/dmoz-utils/pkg/articletext"
	ccsv "github.com/lucmichalski/dmoz-utils/pkg/csv"
	"github.com/lucmichalski/dmoz-utils/pkg/gowap"
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
	isSitemap    bool
	isHostUpdate bool
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
	pflag.BoolVarP(&isHostUpdate, "host-update", "", false, "update database with host and scheme")
	pflag.BoolVarP(&isSitemap, "sitemap", "", false, "extract sitemaps from robots.txt files")
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
		DB.AutoMigrate(&AlexaWebsite{})
		DB.AutoMigrate(&Website{})
		DB.AutoMigrate(&Category{})
		DB.AutoMigrate(&Rss{})
		DB.AutoMigrate(&Rank{})
		DB.AutoMigrate(&Sitemap{})
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
		website.IndexAttrs("ID", "Link", "Path", "Domain", "Tld")

		category := Admin.AddResource(&Category{}, &admin.Config{Menu: []string{"Website Management"}, Priority: -3})
		category.Meta(&admin.Meta{Name: "Categories", Type: "select_many"})

		alexaWebsite := Admin.AddResource(&AlexaWebsite{}, &admin.Config{Menu: []string{"Website Management"}, Priority: -1})
		alexaWebsite.IndexAttrs("ID", "Link", "Path", "Domain", "Tld")

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
	if isImportRDF {
		importRdf("./shared/dataset/content.rdf.u8", DB)
	}
	if isLoadData {
		loadData("./dmoz_dataset.csv", DB)
	}
	if isScanFeeds {
		scanFeeds(DB)
	}

	if isSitemap {
		scanSitemap(DB)
	}

	if isHostUpdate {
		scanHost(DB)
	}

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

func scanHost(DB *gorm.DB) {
	offset := isOffset * isLimit

	type result struct {
		Link string
	}
	var results []result
	query := fmt.Sprintf("select link FROM websites WHERE analyzed=1 AND status_code=200 AND tld IS NULL ORDER BY RAND() LIMIT %d,%d", offset, isLimit)
	fmt.Println("query:", query)

	t := throttler.New(36, 100000000)

	DB.Raw(query).Scan(&results)
	for _, r := range results {
		go func(entry result) error {
			defer t.Done(nil)
			fmt.Println("entry.Link:", entry.Link)
			website := &Website{}
			if !DB.Where("link = ? AND tld IS NULL", entry.Link).First(&website).RecordNotFound() {
				u, err := url.Parse(entry.Link)
				if err != nil {
					return err
				}
				website.Host = u.Host
				website.Scheme = u.Scheme
				t, err := tld.Parse(entry.Link)
				if err != nil {
					return err
				}
				website.Domain = t.Domain
				website.Tld = t.TLD
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

func scanSitemap(DB *gorm.DB) {
	offset := isOffset * isLimit

	type result struct {
		Link string
	}
	var results []result
	query := fmt.Sprintf("select link FROM websites WHERE analyzed=1 AND status_code=200 AND article_text IS NULL ORDER BY RAND() LIMIT %d,%d", offset, isLimit)
	fmt.Println("query:", query)

	t := throttler.New(36, 100000000)

	DB.Raw(query).Scan(&results)
	for _, r := range results {
		go func(entry result) error {
			defer t.Done(nil)
			fmt.Println("entry.Link:", entry.Link)
			if strings.HasPrefix(entry.Link, "http") {
				website := &Website{}
				if !DB.Where("link = ? AND article_text IS NULL", entry.Link).First(&website).RecordNotFound() {
					// get summary
					text, err := articletext.GetArticleTextFromUrl(entry.Link)
					if err != nil {
						return err
					}
					if text != "" {
						website.ArticleText = text
					}
					// check robots.txt
					robotsTxtLink := fmt.Sprintf("%s/robots.txt", entry.Link)
					robotsTxtLink = strings.Replace(robotsTxtLink, "//robots.txt", "/robots.txt", -1)
					// download content
					content, err := downloadRobotsTxt(robotsTxtLink)
					if err == nil {
						if content != "" {
							// parse robots.txt
							robots, err := robotstxt.Parse(content, robotsTxtLink)
							if err == nil {
								if !strings.Contains(content, "<html") {
									website.RobotsTxt = content
									// strings.Join(robots.Sitemaps(), ",")
									for _, sitemap := range robots.Sitemaps() {
										s := Sitemap{Href: sitemap}
										if strings.HasSuffix(sitemap, ".gz") {
											s.Gziped = true
										}
										if strings.Contains(sitemap, "index") {
											s.Index = true
										}
										website.Sitemaps = append(website.Sitemaps, s)
									}
								}
							}
						}
					}
					// save website
					if err := DB.Save(website).Error; err != nil {
						return err
					}
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

func downloadRobotsTxt(rawUrl string) (string, error) {
	// Get the data
	resp, err := http.Get(rawUrl)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 || resp.StatusCode == 403 || resp.StatusCode == 401 {
		return "", fmt.Errorf("not exists")
	}
	// read all
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
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
	// colly.CacheDir(cachePath),
	)

	if isTorProxy {
		rp, err := proxy.RoundRobinProxySwitcher("socks5://127.0.0.1:5566", "socks5://127.0.0.1:8119")
		if err != nil {
			log.Fatal(err)
		}
		c.SetProxyFunc(rp)
	}

	wapp, err := gowap.Init("./apps.json", false)
	if err != nil {
		log.Fatal(err)
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
		website := &Website{}
		if !DB.Where("link = ?", r.Request.URL.String()).First(&website).RecordNotFound() {
			website.Alive = false
			website.StatusCode = r.StatusCode
			website.Analyzed = 1
			if err := DB.Save(website).Error; err != nil {
				log.Fatalln("could not update entry: ", err)
			}
		}
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
		website := &Website{}
		if !DB.Where("link = ? AND analyzed=0", e.Request.Ctx.Get("url")).First(&website).RecordNotFound() {

			e.ForEach(`title`, func(_ int, el *colly.HTMLElement) {
				website.Title = el.Text
			})

			e.ForEach(`meta[name="description"]`, func(_ int, el *colly.HTMLElement) {
				website.Description = el.Attr("content")
			})

			e.ForEach(`meta[property="og:description"]`, func(_ int, el *colly.HTMLElement) {
				if website.Description == "" {
					website.Description = el.Attr("content")
				}
			})

			e.ForEach(`link[type="application/rss+xml"]`, func(_ int, el *colly.HTMLElement) {
				rss := el.Attr("href")
				if rss != "" {
					if !strings.HasPrefix(rss, "http") {
						separator := "/"
						if strings.HasSuffix(e.Request.Ctx.Get("url"), "/") && strings.HasPrefix(rss, "/") {
							separator = ""
						}
						rss = e.Request.Ctx.Get("url") + separator + rss
					}
					website.Rss = append(website.Rss, Rss{Href: rss})
				}
			})

			if res, err := wapp.Analyze(e.Request.Ctx.Get("url")); err == nil {
				prettyJSON, err := json.Marshal(res)
				if err != nil {
					log.Warnln("prettyJSON:", err)
				}
				website.Wap = string(prettyJSON)
			}
			website.StatusCode = 200
			website.Analyzed = 1

			// Update entry
			if err := DB.Save(website).Error; err != nil {
				log.Fatalln("could not update entry: msg=", err, "url=", e.Request.Ctx.Get("url"))
			}
		}

	})

	type res struct {
		Link string
	}

	// Load list of websites.
	var results []res
	offset := isOffset * isLimit

	query := fmt.Sprintf("select link FROM websites WHERE analyzed=0 ORDER BY RAND() LIMIT %d,%d", offset, isLimit)
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
	TextExtract string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	ArticleText string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	RobotsTxt   string   `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Host        string
	Scheme      string
	Domain      string
	Tld         string
	Ranking     Rank
	Rss         []Rss
	Sitemaps    []Sitemap
}

type AlexaWebsite struct {
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
	TextExtract string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	ArticleText string   `gorm:"type:longblob; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longblob"`
	RobotsTxt   string   `gorm:"type:longtext; CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci" sql:"type:longtext"`
	Host        string
	Scheme      string
	Domain      string
	Tld         string
	Ranking     Rank
	Rss         []Rss
	Sitemaps    []Sitemap
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
