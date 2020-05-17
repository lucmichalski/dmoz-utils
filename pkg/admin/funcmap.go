package admin

import (
	"html/template"

	"github.com/qor/admin"
)

func initFuncMap(Admin *admin.Admin) {
	Admin.RegisterFuncMap("render_latest_articles", renderLatestArticles)
	Admin.RegisterFuncMap("render_latest_feeds", renderLatestFeeds)
}

func renderLatestArticles(context *admin.Context) template.HTML {
	var articleContext = context.NewResourceContext("Article")
	articleContext.Searcher.Pagination.PerPage = 25
	if articles, err := articleContext.FindMany(); err == nil {
		return articleContext.Render("index/table", articles)
	}
	return template.HTML("")
}

func renderLatestFeeds(context *admin.Context) template.HTML {
	var feedContext = context.NewResourceContext("Feed")
	feedContext.Searcher.Pagination.PerPage = 25
	if feeds, err := feedContext.FindMany(); err == nil {
		return feedContext.Render("index/table", feeds)
	}
	return template.HTML("")
}
