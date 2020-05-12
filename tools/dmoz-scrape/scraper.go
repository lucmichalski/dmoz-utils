package main

import (
    "os"
    "math"
    "bytes"
    "io"
    "unicode/utf8"
    "strings"
    "encoding/json"
    "net"
    "net/http"
    "io/ioutil"
    "fmt"
    "log"
    "regexp"
    "runtime"
    "time"
    urllib "net/url"
    "database/sql"
    "github.com/vmihailenco/redis"
    _ "github.com/bmizerany/pq"
    "code.google.com/p/go-charset/charset"
    _ "code.google.com/p/go-charset/data"
    "github.com/saintfish/chardet"
)

type RedisConfig struct {Host string; Password string; DB int64; ConnPoolSize int;}
type PostgresConfig struct {DBName string; User string; Password string; Host string; Port int64; SSLMode string}
type ScraperConfig struct {QueueTimeout int64; UserAgent string}
type Config struct {Redis RedisConfig; Postgres PostgresConfig; Scraper ScraperConfig}

var (
    // regex is case-insensitive and counts newlines in `.`
    titleRegex = regexp.MustCompile("(?is)<title\\s*(?:[A-Za-z]+=[\"'][A-Za-z0-9_ -]+[\"'])?>([^<]+)</title>")
    metaEquiv = regexp.MustCompile("(?is)<meta\\s+http-equiv=[\"']?refresh[\"']?\\s+.+url=\\s*([^\"']+)")
    charsetRegex = regexp.MustCompile("(?i)charset=([A-Za-z0-9-]+)")

    numCPU = int(math.Max(float64(runtime.NumCPU()), 4))
    workPool = make(chan bool, int(math.Max(float64(numCPU*4), 20)))

    scraperConfig = ScraperConfig {QueueTimeout: 10, UserAgent: "titleScraper/1.0"}

    redisConfig = RedisConfig {Host: "localhost:6379", Password: "", DB: -1, ConnPoolSize: 10}
    redisConn *redis.Client

    pgConfig = PostgresConfig {DBName: "titlescraper", Password: "", User: "", Port: 5432, SSLMode: "disable"}
    pgConn *sql.DB

    chartypeSet = make(map[string]bool)
)

func readConfig() {
    file, err := os.Open("settings.json")
    defer file.Close()

    if err != nil {
        log.Println("No settings.json file found. Using defaults.")
    } else {
        data, err := ioutil.ReadAll(file)
        if err != nil {
            log.Println("Cannot read settings.json. Using defaults.")
        } else {
            var configObj Config
            err := json.Unmarshal(data, &configObj)
            if err != nil {
                log.Println("settings.json is invalid. Using defaults.")
            } else {
                redisConfig = configObj.Redis
                pgConfig = configObj.Postgres
                scraperConfig = configObj.Scraper
            }
        }
    }
}

func timeoutDialler(timeout time.Duration) func(net, addr string) (client net.Conn, err error) {
    return func(netw, addr string) (net.Conn, error) {
        client, err := net.DialTimeout(netw, addr, time.Duration(30*time.Second))
        if err != nil {
            return nil, err
        }
        client.SetDeadline(time.Now().Add(timeout))
        return client, nil
    }
}

func fetchPage(url string) string {
    req, err := http.NewRequest("GET", url, nil)
    req.Header.Set("User-Agent", scraperConfig.UserAgent)

    httpClient := http.Client{
        Transport: &http.Transport{
            Dial: timeoutDialler(time.Duration(10*time.Second)),
            DisableKeepAlives: true,
        },
    }

    resp, err := httpClient.Do(req)
    if err != nil {
        log.Println("HTTP_ERROR:", err)
        return ""
    }
    defer resp.Body.Close()

    if resp.StatusCode == 200 {
        var dataStream io.Reader

        switch charType := fetchCharset(resp.Header.Get("Content-Type")); {

        case charType == "utf-8":
            dataStream = resp.Body

        case chartypeSet[charType]:
            // charset in available list for conversion
            charsetStream, err := charset.NewReader(charType, resp.Body)
            if err != nil {
                log.Println("ENCODING_ERROR:", err)
            } else {
                dataStream = charsetStream
            }

        default:
            //need to guess chartype
            bodyBytes, err := ioutil.ReadAll(resp.Body)
            if err != nil {
                log.Println("IO_ERROR:", err)
            }

            detector := chardet.NewHtmlDetector()
            result, err := detector.DetectBest(bodyBytes)
            if err != nil {
                log.Println("ENCODING_ERROR no_known_encoding", url)
                return ""
            }

            charType = strings.ToLower(result.Charset)
            if chartypeSet[charType] {
                dataStream = bytes.NewReader(bodyBytes)
                charsetStream, err := charset.NewReader(charType, dataStream)
                if err != nil {
                    log.Println("ENCODING_ERROR:", err)
                } else {
                    dataStream = charsetStream
                }
            }
        }

        if dataStream != nil {
            var bodyBytes []byte
            bodyBytes, err := ioutil.ReadAll(dataStream)
            if err != nil {
                log.Println("ERROR:", err)
            }

            return string(bodyBytes)
        } else {
            log.Println("ENCODING_ERROR: no suitable encoding found for", url)
        }
    }
    return ""
}

func fetchTitle(bodyText string) string {
    if bodyText != "" {
        matches := titleRegex.FindStringSubmatch(bodyText)
        if matches != nil {
            return strings.TrimSpace(matches[1])
        }
    }
    return ""
}

func fetchMetaRedirect(bodyText string, url string) string {
    if bodyText != "" {
        matches := metaEquiv.FindStringSubmatch(bodyText)
        if matches != nil {
            uri,err := urllib.Parse(matches[1])
            if err != nil {
                log.Println("URL_ERROR cannot parse", matches[1])
                return ""
            }

            if uri.Host != "" {
                // redirect url is fully qualified
                return matches[1]
            } else {
                // needs stitching
                origUri,err := urllib.Parse(url)
                if err != nil {
                    log.Println("URL_ERROR cannot parse", matches[1])
                    return ""
                }

                uri.Host = origUri.Host
                uri.Scheme = origUri.Scheme
                if !strings.HasPrefix(uri.Path, "/") {
                    uri.Path = "/" + uri.Path
                }
                return uri.String()
            }
        }
    }
    return ""
}

func fetchCharset(header string) string {
    if header != "" {
        matches := charsetRegex.FindStringSubmatch(header)
        if matches != nil {
            return strings.ToLower(matches[1])
        }
    }
    return ""
}

func fetchTitleJob(url string) {
    bodyText := fetchPage(url)
    if bodyText != "" {
        title := fetchTitle(bodyText)
        if title != "" {
            validTitle := utf8.ValidString(title)
            //validBodyText := utf8.ValidString(bodyText)

            if !validTitle {
                log.Println("CHARSET_ERROR title", url)
            }

            /*
            if !validBodyText {
                log.Println("CHARSET_ERROR body", url)
            }
            */

            //if validTitle && validBodyText {
            if validTitle {
                pg := getPGConn()
                //_, err := pg.Exec("UPDATE categorized_pages SET web_title = $1, web_body = $2 WHERE url = $3", title, bodyText, url)
                _, err := pg.Exec("UPDATE categorized_pages SET web_title = $1 WHERE url = $2", title, url)
                if err != nil {
                    log.Println("SQL_ERROR url:", url, err)
                }
            }
        } else {
            metaRefreshUrl := fetchMetaRedirect(bodyText, url)
            if metaRefreshUrl == "" {
                log.Println("TITLE_ERROR title_not_found", url)
            } else {
                fetchTitleJob(metaRefreshUrl)
            }
        }
    }

    <-workPool
}

func getRedisConn() *redis.Client {
    if redisConn == nil {
        redisConn := redis.NewTCPClient(redisConfig.Host, redisConfig.Password, redisConfig.DB)
        redisConn.ConnPool.(*redis.MultiConnPool).MaxCap = redisConfig.ConnPoolSize
        defer redisConn.Close()
        return redisConn
    }
    return redisConn
}

func getPGConn() *sql.DB {
    if pgConn == nil {
        var err error
        pgConn, err = sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s", pgConfig.Host, pgConfig.Port, pgConfig.User, pgConfig.Password, pgConfig.DBName, pgConfig.SSLMode))
        if err != nil {
            log.Fatal("DB ERROR:", err)
        }
        return pgConn
    }
    return pgConn
}

func main() {
    readConfig()

    // load up available charsets
    for _,name := range charset.Names() {
        chartypeSet[name] = true
    }

    log.Printf("Starting with %d processes\n", numCPU)
    runtime.GOMAXPROCS(numCPU)

    conn := getRedisConn()

    for {
        obj := conn.BLPop(scraperConfig.QueueTimeout, "urljobs")
        err := obj.Err()
        if err != nil {
            if err == redis.Nil {
                log.Printf("No job seen for %d seconds. Exiting.\n", scraperConfig.QueueTimeout)
                break
            } else {
                log.Fatalf("ERROR: %s\n", err)
            }
        } else {
            url := obj.Val()[1]
            workPool <- true
            go fetchTitleJob(url)
        }
    }
}
