import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import unquote

BASE_URL = "https://en.wikipedia.org"
START_ARTICLE = "/wiki/Big_data"
MAX_PAGES = 100
OUTPUT_DIR = "../data/wiki"
SLEEP_TIME = 1.0


def is_valid_wiki_link(href: str) -> bool:
    if not href:
        return False
    if not href.startswith("/wiki/"):
        return False
    if any(prefix in href for prefix in [
        ":", "#", "Main_Page"
    ]):
        return False
    return True


def article_name_from_href(href: str) -> str:
    return unquote(href.split("/wiki/")[1])


def save_html(filename: str, html: str):
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html)


def rewrite_links_to_local(soup, downloaded_articles):
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/wiki/"):
            name = article_name_from_href(href)
            if name in downloaded_articles:
                a["href"] = f"{name}.html"


def extract_article_links(soup):
    content = soup.find("div", {"id": "mw-content-text"})
    links = set()

    if not content:
        return links

    for a in content.find_all("a", href=True):
        href = a["href"]
        if is_valid_wiki_link(href):
            links.add(href)

    return links


def crawl():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    queue = [START_ARTICLE]
    visited = set()

    downloaded_articles = {}

    while queue and len(visited) < MAX_PAGES:
        href = queue.pop(0)
        article = article_name_from_href(href)

        if article in visited:
            continue

        url = BASE_URL + href
        print(f"Downloading: {url}")

        headers = {
            "User-Agent": "MiniSearchCrawler/1.0 (Educational project)"
        }
        response = requests.get(url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to download {url}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")

        visited.add(article)
        downloaded_articles[article] = soup

        links = extract_article_links(soup)

        for link in links:
            name = article_name_from_href(link)
            if name not in visited and link not in queue:
                queue.append(link)

        time.sleep(SLEEP_TIME)

    for article, soup in downloaded_articles.items():
        rewrite_links_to_local(soup, downloaded_articles)
        filename = os.path.join(OUTPUT_DIR, f"{article}.html")
        save_html(filename, str(soup))
        print(f"Saved: {filename}")


if __name__ == "__main__":
    crawl()
