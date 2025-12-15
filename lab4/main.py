import os
import sqlite3
from collections import defaultdict

from src.parser import parse_html, init_db
from src.graph_builder import build_graph
from src.PageRank_MapReduce import pagerank_mapreduce
from src.PageRank_Pregel import pagerank_pregel
from src.search import term_at_a_time, document_at_a_time

DB_PATH = "db/search.db"
DATA_DIR = "data/wiki"
GRAPH_DIR = "graph"

def main():
    os.makedirs("db", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    html_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".html")]
    for filename in html_files:
        doc_id = filename.replace(".html", "")
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO documents(id) VALUES (?)", (doc_id,))
        words, links = parse_html(os.path.join(DATA_DIR, filename))
        term_freq = defaultdict(int)
        for w in words: term_freq[w] += 1
        for term, tf in term_freq.items():
            cursor.execute("INSERT OR IGNORE INTO terms(term) VALUES (?)", (term,))
            cursor.execute("SELECT id FROM terms WHERE term = ?", (term,))
            term_id = cursor.fetchone()[0]
            cursor.execute("INSERT OR REPLACE INTO doc_terms(doc_id, term_id, tf) VALUES (?, ?, ?)", (doc_id, term_id, tf))
        for to_doc in links:
            cursor.execute("INSERT INTO links(from_doc, to_doc) VALUES (?, ?)", (doc_id, to_doc))
    conn.commit()

    build_graph()

    pr_map = pagerank_mapreduce()
    pr_pregel = pagerank_pregel()


    query = input("\nEnter your search query: ").lower()
    query_terms = [t for t in query.split() if t.isalpha()]

    results_taat = term_at_a_time(conn, query_terms, pr_map)
    results_taat = sorted(results_taat, key=lambda x: (-x[1], x[0]))

    results_daat = document_at_a_time(conn, query_terms, pr_map)

    print("\n--- Search results: TAAT ---")
    for doc, score in results_taat:
        print(f"{doc}: {score:.4f}")

    print("\n--- Search results: DAAT ---")
    for doc, score in results_daat:
        print(f"{doc}: {score:.4f}")

    conn.close()

if __name__ == "__main__":
    main()
