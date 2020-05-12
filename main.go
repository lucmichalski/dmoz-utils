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
	"github.com/jinzhu/gorm"
	// "github.com/k0kubun/pp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/qor/admin"
	"github.com/qor/assetfs"
	"github.com/qor/qor/utils"
	"github.com/qor/validations"
	"github.com/spf13/pflag"
)

var (
	isHelp       bool
	isVerbose    bool
	isAdmin      bool
	isDataset    bool
	parallelJobs int
)

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

	DB, _ := gorm.Open("sqlite3", "dmoz.db")
	DB.AutoMigrate(&Website{}, &Category{}, &Rss{}, &Rank{})

	validations.RegisterCallbacks(DB)
	// media.RegisterCallbacks(DB)

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
		Admin.AddResource(&Website{}, &admin.Config{Menu: []string{"Website Management"}, Priority: -1})
		category := Admin.AddResource(&Category{}, &admin.Config{Menu: []string{"Website Management"}, Priority: -3})
		category.Meta(&admin.Meta{Name: "Categories", Type: "select_many"})

		// initalize an HTTP request multiplexer
		mux := http.NewServeMux()

		// Mount admin interface to mux
		Admin.MountTo("/admin", mux)

		fmt.Println("Listening on: 9001")
		http.ListenAndServe(":9001", mux)
	}

	// import data
	importRdf("./shared/dataset/content.rdf.u8", DB)

}

func importRdf(rdfFile string, DB *gorm.DB) {
	// var dmozs []Dmoz

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

	for _, entry := range root.SelectElements("Topic") {
		c := &Category{}

		// pp.Println(entry)

		//catid
		catId := entry.SelectElement("catid")
		if catId != nil {
			log.Println("catid is:", catId.Text())
			c.Code = catId.Text()
		} else {
			continue
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
		cc, err := createOrUpdateCategory(DB, c)
		checkErr(err)

		//link1
		link1 := entry.SelectElement("link1")
		if link1 != nil {
			url := link1.SelectAttr("r:resource").Value
			// pp.Println(link1)
			log.Println("url is:", url)
			website := &Website{}
			website.Category = *cc
			website.Link = url
			website.Path = topic
			createOrUpdateWebsite(DB, website)
		}

		//link
		links := entry.SelectElements("link")
		if links != nil {
			for _, url := range links {
				// pp.Println(url)
				urltxt := url.SelectAttr("r:resource").Value
				log.Println("url is:", urltxt)
				website := &Website{}
				website.Category = *cc
				website.Link = urltxt
				website.Path = topic
				createOrUpdateWebsite(DB, website)
			}
			// d.Link = linkarr
		}
		// dmozs = append(dmozs, d)
	}

	log.Println("Finish Parsing RDF xml, time period:", time.Now().Sub(starttime))
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
	Link        string
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
	Name string
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
