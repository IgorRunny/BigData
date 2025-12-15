import os
import re
import sqlite3
from collections import Counter
from bs4 import BeautifulSoup

DATA_DIR = "data/wiki"

def tokenize(text: str):
    text = text.lower()
    return re.findall(r"[a-z]{2,}", text)

def init_db(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS terms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT UNIQUE
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS doc_terms (
            doc_id TEXT,
            term_id INTEGER,
            tf INTEGER,
            PRIMARY KEY (doc_id, term_id)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS links (
            from_doc TEXT,
            to_doc TEXT
        )
    """)
    conn.commit()

def parse_html(filepath: str):
    with open(filepath, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")
    content = soup.find("div", {"id": "mw-content-text"})
    if not content:
        return [], []
    text = " ".join(p.get_text() for p in content.find_all("p"))
    words = tokenize(text)
    links = []
    for a in content.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".html"):
            links.append(href.replace(".html", ""))
    return words, links
