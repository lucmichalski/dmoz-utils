package main

import (
    "os"
    "io/ioutil"
    "fmt"
    "log"
    "encoding/csv"
    "encoding/json"
    "strings"
    "github.com/vmihailenco/redis"
)

type RedisConfig struct {Host string; Password string; DB int64; ConnPoolSize int;}
type PostgresConfig struct {DBName string; User string; Password string; Host string; Port int64; SSLMode string}
type Config struct {Redis RedisConfig; Postgres PostgresConfig}

var (
    redisConfig = RedisConfig {Host: "localhost:6379", Password: "", DB: -1, ConnPoolSize: 10}
    redisConn *redis.Client
    domainBlacklist = make(map[string] bool)
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
            }
        }
    }
}

func getRedisConn()  *redis.Client {
    if redisConn == nil {
        redisConn := redis.NewTCPClient(redisConfig.Host, redisConfig.Password, redisConfig.DB)
        defer redisConn.Close()
        return redisConn
    }
    return redisConn
}

func main() {
    domainBlacklist["Adult"] = true
    domainBlacklist["World"] = true
    domainBlacklist["Regional"] = true
    readConfig()

    fmt.Println("opening file", os.Args[1])
    file, err := os.Open(os.Args[1])
    if err != nil {
        log.Fatal(err)
        return
    }
    defer file.Close()

    csvStream := csv.NewReader(file)

    conn := getRedisConn()
    multi, err := conn.MultiClient()
    defer multi.Close()

    for line, err := csvStream.Read(); err == nil; line, err = csvStream.Read() {
        url := line[0]
        cats := line[3]

        catSegments := strings.Split(cats, "/")

        if !domainBlacklist[catSegments[1]] {
            reqs, transErr := multi.Exec(func() {
                if !multi.SIsMember("urlset", url).Val() {
                    multi.RPush("urljobs", url)
                    multi.SAdd("urlset", url)
                }
            })
            if transErr == redis.Nil {
                fmt.Println("TRANSACTION_ERR ", err)
                fmt.Println("REQS ", reqs)
            }
        }
    }
}
