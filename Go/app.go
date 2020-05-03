package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// Movie struct
type Movie struct {
	Title    string   `json:"title"`
	Summary  string   `json:"summary"`
	Director string   `json:"director"`
	Country  string   `json:"country"`
	Actors   []string `json:"actors"`
	Genre    []string `json:"genre"`
	Date     string   `json:"date"`
	Src      string   `json:"src"`
	URL      string   `json:"url"`
}

// Get url links for imdb top 250 movies
func getMovieUrls() []string {
	url := "https://www.imdb.com/chart/top/"
	response, _ := http.Get(url)
	doc, _ := goquery.NewDocumentFromResponse(response)
	urls := []string{}
	doc.Find("tbody > tr > td.titleColumn > a").Each(func(i int, selection *goquery.Selection) {
		href, _ := selection.Attr("href")
		urls = append(urls, `https://www.imdb.com`+href)
	})
	return urls
}

// Given any movie url, scrape the useful information:
// title, summary, director, country, actors, genre, date, src, url
func scrapeMovie(url string) Movie {
	response, _ := http.Get(url)
	doc, _ := goquery.NewDocumentFromResponse(response)
	re := regexp.MustCompile(`\(\d+\)`)
	title := re.ReplaceAllString(doc.Find("div.title_wrapper > h1").Text(), "")
	title = strings.ReplaceAll(title, "\u00a0", "")
	title = strings.Trim(title, " ")
	summary := doc.Find("div.summary_text").Text()
	summary = strings.ReplaceAll(summary, "\n", "")
	summary = strings.Trim(summary, " ")

	director := ""
	actors := []string{}
	doc.Find("div.credit_summary_item").Each(func(i int, selection *goquery.Selection) {
		if i == 0 {
			director = selection.Text()
			director = strings.ReplaceAll(director, "Director:", "")
			director = strings.ReplaceAll(director, "\n", "")
			director = strings.Trim(director, " ")
		} else if i == 2 {
			temp := strings.Split(selection.Text(), "|")[0]
			temp = strings.ReplaceAll(temp, "Stars:", "")
			temp = strings.Trim(temp, " ")
			actorTemp := strings.Split(temp, ",")
			for _, i := range actorTemp {
				i = strings.ReplaceAll(i, "\n", "")
				i = strings.Trim(i, " ")
				actors = append(actors, i)
			}
		}
	})

	genre := []string{}
	date := ""
	country := ""

	subtexts := strings.Split(doc.Find("div.subtext").Text(), "|")

	genreTemp := strings.Split(subtexts[len(subtexts)-2], ",")
	for _, i := range genreTemp {
		i = strings.ReplaceAll(i, "\n", "")
		i = strings.Trim(i, " ")
		genre = append(genre, i)
	}

	dateString := subtexts[len(subtexts)-1]
	dateString = strings.ReplaceAll(dateString, "\n", "")
	re = regexp.MustCompile(`\(.+\)`)
	dateString = re.ReplaceAllString(dateString, "")
	dateString = strings.Trim(dateString, " ")
	tm, _ := time.Parse("02 January 2006", dateString)
	date = tm.Format("2006-01-02")

	countryString := subtexts[len(subtexts)-1]
	countryString = strings.Split(countryString, "(")[1]
	countryString = strings.ReplaceAll(countryString, "\n", "")
	countryString = strings.ReplaceAll(countryString, ")", "")
	country = strings.Trim(countryString, " ")

	src, _ := doc.Find("div.poster > a > img").Attr("src")

	movie := Movie{title, summary, director, country, actors, genre, date, src, url}
	return movie
}

// producer, put all urls into one channel
func produce(urls []string) chan string {
	in := make(chan string, 250)
	go func() {
		for _, url := range urls {
			in <- url
		}
		close(in)
	}()
	return in
}

// comsumer, get url from producer channel and scrape
func consume(in chan string) chan Movie {
	out := make(chan Movie, 250)
	go func() {
		for url := range in {
			out <- scrapeMovie(url)
		}
		close(out)
	}()
	return out
}

// merge the result of different comsumers
func merge(cs ...chan Movie) <-chan Movie {
	var wg sync.WaitGroup
	out := make(chan Movie, 250)

	output := func(c chan Movie) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Save scraped data into json format for Elastic Search Bulk insert
func saveJSON(movies []Movie) {
	doc := ""
	for _, m := range movies {
		doc += `{"index":{"_id": "` + strings.Split(m.URL, "/")[4] + `"}}` + "\n"

		buf := bytes.NewBuffer([]byte{})
		jsonEncoder := json.NewEncoder(buf)
		jsonEncoder.SetEscapeHTML(false)
		jsonEncoder.Encode(m)

		doc += buf.String()
	}
	_ = ioutil.WriteFile("./movies.json", []byte(doc), 0644)
}

func main() {
	start := time.Now()
	urls := getMovieUrls()
	in := produce(urls)

	outs := []chan Movie{}
	for i := 0; i < 40; i++ {
		outs = append(outs, consume(in))
	}
	out := merge(outs...)
	movies := []Movie{}
	for m := range out {
		movies = append(movies, m)
	}
	fmt.Println(len(movies))
	elapsed := time.Since(start)
	fmt.Printf("took %s", elapsed)
	saveJSON(movies)
}
