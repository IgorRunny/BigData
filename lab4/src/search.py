import sqlite3

ALPHA = 0.1

def load_pagerank(filepath):
    pagerank = {}
    with open(filepath, encoding="utf-8") as f:
        for line in f:
            doc, rank = line.strip().split()
            pagerank[doc] = float(rank)
    return pagerank

def term_at_a_time(conn, query_terms, pagerank):
    cursor = conn.cursor()
    score = {}
    for term in query_terms:
        cursor.execute("SELECT id FROM terms WHERE term = ?", (term,))
        row = cursor.fetchone()
        if not row: continue
        term_id = row[0]
        cursor.execute("SELECT doc_id, tf FROM doc_terms WHERE term_id = ?", (term_id,))
        for doc_id, tf in cursor.fetchall():
            score[doc_id] = score.get(doc_id, 0) + tf
    for doc in score:
        score[doc] += ALPHA * pagerank.get(doc, 0)
    return sorted(score.items(), key=lambda x: -x[1])

def document_at_a_time(conn, query_terms, pagerank):
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM documents")
    all_docs = [row[0] for row in cursor.fetchall()]
    score = {}
    for doc in all_docs:
        total = 0
        for term in query_terms:
            cursor.execute("SELECT id FROM terms WHERE term = ?", (term,))
            row = cursor.fetchone()
            if not row: continue
            term_id = row[0]
            cursor.execute("SELECT tf FROM doc_terms WHERE doc_id = ? AND term_id = ?", (doc, term_id))
            row = cursor.fetchone()
            if row: total += row[0]
        if total > 0: score[doc] = total + ALPHA * pagerank.get(doc, 0)
    return sorted(score.items(), key=lambda x: -x[1])
