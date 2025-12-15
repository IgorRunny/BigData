import os
from collections import defaultdict

GRAPH_DIR = "graph"
DAMPING = 0.85
SUPERSTEPS = 10

class Vertex:
    def __init__(self, vid, out_links, num_vertices):
        self.id = vid
        self.out_links = out_links
        self.rank = 1.0 / num_vertices

    def compute(self, messages, num_vertices):
        incoming_sum = sum(messages)
        self.rank = (1 - DAMPING) / num_vertices + DAMPING * incoming_sum

    def send_messages(self):
        if not self.out_links:
            return []
        share = self.rank / len(self.out_links)
        return [(dst, share) for dst in self.out_links]

def load_graph():
    with open(os.path.join(GRAPH_DIR, "nodes.txt")) as f:
        nodes = [line.strip() for line in f]
    edges = defaultdict(list)
    with open(os.path.join(GRAPH_DIR, "edges.txt")) as f:
        for line in f:
            src, dst = line.strip().split()
            edges[src].append(dst)
    return nodes, edges

def pagerank_pregel():
    nodes, edges = load_graph()
    num_vertices = len(nodes)
    vertices = {node: Vertex(node, edges.get(node, []), num_vertices) for node in nodes}
    for _ in range(SUPERSTEPS):
        messages = defaultdict(list)
        for v in vertices.values():
            for dst, value in v.send_messages():
                messages[dst].append(value)
        dangling_rank = sum(v.rank for v in vertices.values() if not v.out_links)
        dangling_share = dangling_rank / num_vertices
        for v in vertices.values():
            msgs = messages.get(v.id, [])
            v.compute(msgs, num_vertices)
            v.rank += DAMPING * dangling_share
    return {v.id: v.rank for v in vertices.values()}
