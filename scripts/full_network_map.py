#!/usr/bin/env python3
"""
Full Network Map: Builds expanded engagement network from census data.
Includes everyone commenting on 5+ unique scraped profiles, not just registry.
Runs community detection on the expanded graph.
"""

import csv
import os
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CENSUS_FILE = os.path.join(BASE, "data", "analysis", "full-commenter-census.csv")
REGISTRY = os.path.join(BASE, "registry", "profiles.csv")
OUTPUT_PAIRS = os.path.join(BASE, "data", "analysis", "full-network-pairs.csv")
OUTPUT_CLUSTERS = os.path.join(BASE, "data", "analysis", "full-network-clusters.csv")


def load_registry():
    """Load registry slugs."""
    registry = {}
    with open(REGISTRY, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = row["linkedin_url"].rstrip("/").split("?")[0]
            slug = url.split("/")[-1]
            registry[slug] = {
                "profile_id": row["profile_id"],
                "role": row["role"],
                "network": row["network"],
            }
    return registry


def load_census():
    """Load census, filter to 5+ unique authors."""
    commenters = []
    with open(CENSUS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if int(row["unique_authors_commented_on"]) >= 5:
                commenters.append(row)
    return commenters


def build_pairs(commenters):
    """Build commenter->author pairs from census data."""
    pairs = []
    for c in commenters:
        slug = c["commenter_slug"]
        authors_str = c.get("authors_commented_on", "")
        if not authors_str:
            continue

        # We need per-author counts - reload from the census format
        # The census has total but not per-author breakdown in the CSV
        # We'll mark it as present (the count is in the original data)
        authors = authors_str.split(";")
        for author in authors:
            if author:
                pairs.append({
                    "commenter_slug": slug,
                    "author_slug": author,
                })
    return pairs


def build_pairs_with_counts():
    """Build pairs with actual counts by re-reading comments."""
    comments_path = os.path.join(BASE, "data", "scraped", "comments.csv")

    # First get the set of high-freq commenters (5+ authors)
    hf_slugs = set()
    with open(CENSUS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if int(row["unique_authors_commented_on"]) >= 5:
                hf_slugs.add(row["commenter_slug"])

    # Count per pair
    pair_counts = defaultdict(int)
    with open(comments_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug = row["comment_author_slug"]
            if slug in hf_slugs:
                author = row["post_author_slug"]
                pair_counts[(slug, author)] += 1

    pairs = []
    for (commenter, author), count in sorted(pair_counts.items(), key=lambda x: -x[1]):
        pairs.append({
            "commenter_slug": commenter,
            "author_slug": author,
            "comment_count": count,
        })

    return pairs


def detect_clusters(pairs, registry):
    """Simple community detection using label propagation (no external deps)."""
    # Build adjacency: undirected graph of co-engagement
    # Two accounts are connected if they both comment on the same author
    # or one comments on the other

    # Build set of all nodes
    nodes = set()
    for p in pairs:
        nodes.add(p["commenter_slug"])
        nodes.add(p["author_slug"])

    # Build edges: commenter <-> author (weighted by comment count)
    edges = defaultdict(lambda: defaultdict(int))
    for p in pairs:
        c = p["commenter_slug"]
        a = p["author_slug"]
        count = p["comment_count"]
        edges[c][a] += count
        edges[a][c] += count

    # Also connect commenters who share 3+ authors
    commenter_authors = defaultdict(set)
    for p in pairs:
        commenter_authors[p["commenter_slug"]].add(p["author_slug"])

    commenters = list(commenter_authors.keys())
    for i in range(len(commenters)):
        for j in range(i + 1, len(commenters)):
            shared = commenter_authors[commenters[i]] & commenter_authors[commenters[j]]
            if len(shared) >= 3:
                edges[commenters[i]][commenters[j]] += len(shared)
                edges[commenters[j]][commenters[i]] += len(shared)

    # Label propagation clustering
    labels = {node: i for i, node in enumerate(nodes)}

    for iteration in range(50):
        changed = False
        for node in nodes:
            if node not in edges:
                continue

            # Count neighbor labels weighted by edge weight
            label_weights = defaultdict(int)
            for neighbor, weight in edges[node].items():
                label_weights[labels[neighbor]] += weight

            if label_weights:
                best_label = max(label_weights, key=label_weights.get)
                if labels[node] != best_label:
                    labels[node] = best_label
                    changed = True

        if not changed:
            break

    # Renumber clusters from 0
    unique_labels = sorted(set(labels.values()))
    label_map = {old: new for new, old in enumerate(unique_labels)}
    labels = {node: label_map[lab] for node, lab in labels.items()}

    return labels, edges


def write_outputs(pairs, labels, edges, registry):
    """Write network pairs and cluster CSVs."""
    os.makedirs(os.path.dirname(OUTPUT_PAIRS), exist_ok=True)

    # Pairs CSV
    if pairs:
        with open(OUTPUT_PAIRS, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["commenter_slug", "author_slug", "comment_count"])
            writer.writeheader()
            writer.writerows(pairs)

    # Clusters CSV
    cluster_rows = []
    for node, cluster_id in sorted(labels.items(), key=lambda x: (x[1], x[0])):
        degree = len(edges.get(node, {}))
        total_weight = sum(edges.get(node, {}).values())
        in_reg = node in registry
        reg_id = registry[node]["profile_id"] if in_reg else ""
        reg_role = registry[node]["role"] if in_reg else ""

        cluster_rows.append({
            "slug": node,
            "cluster_id": cluster_id,
            "degree": degree,
            "total_edge_weight": total_weight,
            "in_registry": in_reg,
            "registry_id": reg_id,
            "registry_role": reg_role,
        })

    cluster_rows.sort(key=lambda r: (-r["total_edge_weight"]))

    if cluster_rows:
        with open(OUTPUT_CLUSTERS, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cluster_rows[0].keys())
            writer.writeheader()
            writer.writerows(cluster_rows)

    return cluster_rows


def display_results(pairs, labels, cluster_rows, registry):
    """Print network summary."""
    total_nodes = len(labels)
    new_accounts = sum(1 for node in labels if node not in registry)

    # Cluster stats
    clusters = defaultdict(list)
    for row in cluster_rows:
        clusters[row["cluster_id"]].append(row)

    # Filter to meaningful clusters (3+ members)
    meaningful = {cid: members for cid, members in clusters.items() if len(members) >= 3}

    print("=" * 90)
    print("FULL NETWORK MAP")
    print("=" * 90)
    print(f"\nTotal accounts in full network: {total_nodes}")
    print(f"Completely new (not in registry): {new_accounts}")
    print(f"Total directed pairs: {len(pairs):,}")
    print(f"Total clusters: {len(clusters)} ({len(meaningful)} with 3+ members)")

    print(f"\n{'='*90}")
    print("CLUSTER BREAKDOWN (3+ members)")
    print(f"{'='*90}")
    for cid in sorted(meaningful.keys()):
        members = meaningful[cid]
        registered = sum(1 for m in members if m["in_registry"])
        new = len(members) - registered

        # Find hub (highest edge weight)
        hub = max(members, key=lambda m: m["total_edge_weight"])

        print(f"\n  Cluster {cid}: {len(members)} members ({registered} registered, {new} new)")
        print(f"    Hub: {hub['slug']} (weight: {hub['total_edge_weight']})")

        # Top 10 by weight
        top = sorted(members, key=lambda m: m["total_edge_weight"], reverse=True)[:10]
        for m in top:
            reg = m["registry_id"] if m["in_registry"] else "NEW"
            print(f"      {m['slug']:<40} weight={m['total_edge_weight']:>5}  {reg}")

    print(f"\n{'='*90}")
    print("SUMMARY")
    print(f"{'='*90}")
    print(f"  Real ring size (full network): {total_nodes}")
    print(f"  Already registered: {total_nodes - new_accounts}")
    print(f"  Undiscovered accounts: {new_accounts}")


if __name__ == "__main__":
    print("Loading registry...")
    registry = load_registry()
    print(f"Registry: {len(registry)} profiles")

    print("Loading census and building pairs with counts...")
    pairs = build_pairs_with_counts()
    print(f"Pairs: {len(pairs):,}")

    print("Detecting clusters...")
    labels, edges = detect_clusters(pairs, registry)

    print("Writing outputs...")
    cluster_rows = write_outputs(pairs, labels, edges, registry)

    display_results(pairs, labels, cluster_rows, registry)
    print(f"\nOutputs:")
    print(f"  {OUTPUT_PAIRS}")
    print(f"  {OUTPUT_CLUSTERS}")
