
const fs = require("fs")
const cheerio = require("cheerio")
const axios = require("axios")

// Get url links for imdb top 250 movies
async function getMovieUrls() {
    const url = "https://www.imdb.com/chart/top/"
    const response = await axios.get(url)
    const $ = cheerio.load(response.data)
    const urls = $("tbody > tr > td.titleColumn").find("a").toArray().map(element => 
        `https://www.imdb.com${$(element).attr("href")}`)
    return urls
}

// Given any movie url, scrape the useful information:
// title, summary, director, country, actors, genre, date, src, url
async function scrapeMovie(url) {
    movie = {}
    const response = await axios.get(url)
    const $ = cheerio.load(response.data)
    const element = $("div.title_wrapper").find("h1")
    movie["title"] = element.text().replace(/\s\(\d+\)/, "").trim()
    movie["summary"] = $("div.summary_text").text().trim()

    const elements = $("body").find("div.credit_summary_item")
    movie["director"] = $(elements[0]).text().replace("Director:", "").replace("\n", "").trim()
    movie["actors"] = $(elements[2]).text().split("|")[0].replace("Stars:", "").trim().split(",")
    
    const subtexts = $("body").find("div.subtext").text().split("|")
    const length = subtexts.length
    movie["genre"] = subtexts[length-2].split(",").map(t => t.trim())
    movie["date"] = new Date(subtexts[length-1].replace(/\s\(.+\)/, "").trim()).toLocaleString(
        "zh-cn", {year: 'numeric', month: '2-digit', day: '2-digit'})
    movie["country"] = subtexts[length-1].split("(")[1].replace(")", "").trim()

    movie["src"] = $("body").find("div.poster > a > img").attr("src")
    movie["url"] = url
    return movie
}

// For a list of movie urls, divided to k chunks
function getUrlChunks(urls, k=4) {
    const size = Math.ceil((urls.length / k))
    const flag = urls.length % k === 0
    let chunks = [...Array(k).keys()].map(i => urls.slice(size*i, size*(i+1)))
    return chunks
}

// Save scraped data into json format for Elastic Search Bulk insert
function saveJson(movies) {
    let doc = ""
    for(let i=0; i<movies.length; i++) {
        doc += `{"index":{"_id":"${movies[i].url.split("/")[4]}"}\n`
        doc += JSON.stringify(movies[i]) + "\n"
    }
    fs.writeFile("./movies.json", doc, function(err) { 
        if(err) {
            console.log(err);
        }
    })
}


async function main() {
    const start = Date.now()
    const urls = await getMovieUrls()
    const chunks = getUrlChunks(urls, k=50)

    // Asynchronously scrape in K "channels"
    const promises = chunks.map(async(chunk) => {
        let result = []
        for(let i=0; i<chunk.length; i++) {
            const movie = await scrapeMovie(chunk[i])
            result.push(movie)
        }
        return result
    })
  
    const records = await Promise.all(promises)
    const movies = [].concat(...records)
    console.log(movies.length)

    const elapsed = (Date.now() - start) / 1000
    console.log(`took ${elapsed} seconds.`)
    saveJson(movies)
}

main()
