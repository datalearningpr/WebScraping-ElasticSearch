
import time
import math
import json
import requests
import threading
import queue
from datetime import datetime
from bs4 import BeautifulSoup

def getMovieUrls():
    """
    Get url links for imdb top 250 movies
    """
    url = "https://www.imdb.com/chart/top/"
    r = requests.get(url)
    bs = BeautifulSoup(r.text, 'lxml')
    links = bs.select("tbody > tr > td.titleColumn > a")
    urls = [f"https://www.imdb.com{i['href']}" for i in links]
    return urls


def scrapeMovie(url):
    """
    Given any movie url, scrape the useful information:
    title, summary, director, country, actors, genre, date, src, url
    """
    movie = {}
    r = requests.get(url)
    bs = BeautifulSoup(r.text, 'lxml')
    element = bs.select_one("div.title_wrapper > h1")
    element.select_one("span#titleYear").decompose()
    movie["title"] = element.get_text(strip=True)
    movie["summary"] = bs.select_one("div.summary_text").get_text(strip=True)

    elements = bs.select("div.credit_summary_item")
    director = elements[0].get_text(strip=True).replace("Director:", "")
    actors = elements[2].get_text(strip=True).split("|")[0].replace("Stars:", "").split(",")
    movie["director"] = director
    movie["actors"] = actors

    subtexts = bs.select_one("div.subtext").get_text(strip=True).split("|")
    genre = subtexts[-2].split(",")
    movie["genre"] = genre

    date = datetime.strptime(subtexts[-1].split("(")[0].strip(), "%d %B %Y").strftime("%Y-%m-%d")
    country = subtexts[-1].split("(")[1].replace(")", "")
    movie["date"] = date
    movie["country"] = country

    movie["src"] = bs.select_one("div.poster > a > img")["src"]
    movie["url"] = url
    return movie


def task(urls, q):
    """
    Function for each thread to process, scrape a list of movie url
    """
    q.put([scrapeMovie(url) for url in urls])


def getUrlChunks(urls, k=4):
    """
    For a list of movie urls, divided to k chunks
    """
    size = math.ceil(len(urls) / k)
    chunks = [urls[size*i:size*(i+1)] for i in range(k)]
    return chunks

def saveJson(movies):
    """
    Save scraped data into json format for Elastic Search Bulk insert
    """
    doc = ""
    for m in movies:
        id = m["url"].split("/")[-2]
        doc += f"""{{"index":{{"_id":"{id}"}}\n"""
        doc += json.dumps(m) + "\n"

    with open("./movies.json", "w", encoding="utf-8") as f:
        f.write(doc)


if __name__ == "__main__":
    start = time.time() 

    urls = getMovieUrls()
    chunks = getUrlChunks(urls, k=8)

    q = queue.Queue()
    ths = []
    movies = []
    for chunk in chunks:
        th = threading.Thread(target = task, args = (chunk, q))
        th.start()
        ths.append(th)

    for th in ths:
        th.join()

    while not q.empty():
        movies.extend(q.get())

    print(len(movies))
    elapsed = time.time() - start
    print(f"took {elapsed} seconds.")
    saveJson(movies)
