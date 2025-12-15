import os
import sqlite3
from collections import defaultdict

DB_PATH = "db/search.db"
GRAPH_DIR = "graph"

def build_graph():
    os.makedirs(GRAPH_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM documents")
    documents = {row[0] for row in cursor.fetchall()}

    nodes_path = os.path.join(GRAPH_DIR, "nodes.txt")
    with open(nodes_path, "w", encoding="utf-8") as f:
        for doc in sorted(documents):
            f.write(doc + "\n")

    edges_path = os.path.join(GRAPH_DIR, "edges.txt")
    with open(edges_path, "w", encoding="utf-8") as f:
        cursor.execute("SELECT from_doc, to_doc FROM links")
        for from_doc, to_doc in cursor.fetchall():
            if from_doc in documents and to_doc in documents:
                f.write(f"{from_doc} {to_doc}\n")

    conn.close()
