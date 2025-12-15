import os
from collections import defaultdict

GRAPH_DIR = "graph"
DAMPING = 0.85
ITERATIONS = 10

def load_graph():
    with open(os.path.join(GRAPH_DIR, "nodes.txt")) as f:
        nodes = [line.strip() for line in f]
    edges = defaultdict(list)
    with open(os.path.join(GRAPH_DIR, "edges.txt")) as f:
        for line in f:
            src, dst = line.strip().split()
            edges[src].append(dst)
    return nodes, edges

def initialize_ranks(nodes):
    n = len(nodes)
    return {node: 1.0 / n for node in nodes}

def map_phase(ranks, edges):
    contributions = defaultdict(list)
    dangling_rank = 0.0
    for node, rank in ranks.items():
        out_links = edges.get(node, [])
        if out_links:
            share = rank / len(out_links)
            for dst in out_links:
                contributions[dst].append(share)
        else:
            dangling_rank += rank
    return contributions, dangling_rank

def reduce_phase(contributions, dangling_rank, nodes):
    new_ranks = {}
    n = len(nodes)
    dangling_contribution = dangling_rank / n
    for node in nodes:
        incoming_sum = sum(contributions.get(node, []))
        new_ranks[node] = (1 - DAMPING) / n + DAMPING * (incoming_sum + dangling_contribution)
    return new_ranks

def pagerank_mapreduce():
    nodes, edges = load_graph()
    ranks = initialize_ranks(nodes)
    for _ in range(ITERATIONS):
        contributions, dangling_rank = map_phase(ranks, edges)
        ranks = reduce_phase(contributions, dangling_rank, nodes)
    return ranks
