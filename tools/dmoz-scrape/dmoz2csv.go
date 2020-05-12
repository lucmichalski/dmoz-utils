package main

import (
    "os"
    "io"
    "encoding/xml"
    "encoding/csv"
    "log"
    "bufio"
    "fmt"
)

const (
    TAG_DIR_ENTRY = "ExternalPage"
)

type PageData struct {
    XMLName xml.Name `xml:"ExternalPage"`
    URL string `xml:"about,attr"`
    Title string `d:Title>chardata`
    Description string `d:Description>chardata`
    Priority string `xml:"priority">chardata`
    Topic string `xml:"topic">chardata`
}

var (
    docCounter int = 0
    urlSet = make(map[string] bool)
)

func nextPageStart(decoder *xml.Decoder) (xml.StartElement, error) {
    for {
        tok, err := decoder.Token()
        if err != nil {
            /*
            var nilElem xml.StartElement
            log.Println("xml error", err)
            return nilElem, err
            */
            log.Fatal("error", err)
        }
        switch tok := tok.(type) {
        case xml.StartElement:
            if tok.Name.Local == TAG_DIR_ENTRY {
                return tok, nil
            }
        }
    }
    panic("unreachable")
}

func main() {
    file, err := os.Open(os.Args[1])
    if err != nil {
        log.Fatal(err)
        return
    }
    defer file.Close()

    var csvOut io.Writer
    if len(os.Args) > 2 {
        outfile, outErr := os.Create(os.Args[2])
        if outErr != nil {
            log.Fatal(outErr)
            return
        }
        defer outfile.Close()
        csvOut = bufio.NewWriter(outfile)
    } else {
        csvOut = bufio.NewWriter(os.Stdout)
    }

    reader := bufio.NewReader(file)
    xmlReader := xml.NewDecoder(reader)

    for {
        tok, err := nextPageStart(xmlReader)
        if err != nil {
            log.Println("xml error:", err)
            break
        }

        var page PageData
        err = xmlReader.DecodeElement(&page, &tok)
        if err != nil {
            log.Println("error decoding xml: ", err)
            break
        }

        if !urlSet[page.URL] {

            urlSet[page.URL] = true
            csvWriter := csv.NewWriter(csvOut)
            line := []string{page.URL, page.Title, page.Description, page.Topic}
            csvWriter.Write(line)
            csvWriter.Flush()

            docCounter += 1
        }
    }

    fmt.Print("Done! Docs processed:", docCounter)

    if err != nil {
        os.Exit(1)
    }
}
