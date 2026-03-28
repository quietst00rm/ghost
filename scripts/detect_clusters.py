#!/usr/bin/env python3
"""
detect_clusters.py — Community detection on reciprocal engagement network.

Uses Louvain algorithm to identify distinct clusters of accounts that engage
with each other more than with the rest of the network.

Also identifies bridge accounts (profiles with reciprocal pairs in 2+ clusters)
and third-ring expansion candidates.

Outputs:
    data/analysis/clusters.csv          — Per-member cluster assignments
    data/analysis/cluster-summary.csv   — One row per cluster with stats
    data/analysis/bridge-accounts.csv   — Profiles connecting multiple clusters
    data/analysis/third-ring-candidates.csv — Next wave of scrape targets
"""

import csv
from collections import Counter, defaultdict
from pathlib import Path

import community as community_louvain
import networkx as nx

ROOT = Path(__file__).resolve().parent.parent

# WhatsApp pod network identifiers
WHATSAPP_NETWORKS = {"growth-community", "growth-community-extended"}


def load_registry():
    """Load profiles.csv into a dict keyed by slug."""
    profiles = {}
    path = ROOT / "registry" / "profiles.csv"
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("linkedin_url", "").strip()
            if not url:
                continue
            slug = url.rstrip("/").split("/")[-1].lower()
            profiles[slug] = {
                "name": row.get("display_name", ""),
                "role": row.get("role", ""),
                "network": row.get("network", ""),
                "profile_id": row.get("profile_id", ""),
            }
    return profiles


def load_reciprocal_pairs():
    """Load reciprocal-pairs.csv as list of dicts."""
    pairs = []
    path = ROOT / "data" / "analysis" / "reciprocal-pairs.csv"
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["total"] = int(row["total"])
            row["a_to_b_count"] = int(row["a_to_b_count"])
            row["b_to_a_count"] = int(row["b_to_a_count"])
            pairs.append(row)
    return pairs


def build_graph(pairs):
    """Build undirected weighted graph from reciprocal pairs."""
    G = nx.Graph()
    for p in pairs:
        G.add_edge(p["profile_a"], p["profile_b"], weight=p["total"])
    return G


def detect_communities(G):
    """Run Louvain community detection."""
    partition = community_louvain.best_partition(G, weight="weight", random_state=42)
    return partition


def analyze_clusters(G, partition, registry):
    """Compute per-cluster statistics."""
    # Group nodes by cluster
    clusters = defaultdict(list)
    for node, cluster_id in partition.items():
        clusters[cluster_id].append(node)

    summaries = []
    member_rows = []

    for cluster_id in sorted(clusters.keys()):
        members = clusters[cluster_id]
        subgraph = G.subgraph(members)

        # Internal edges (pairs within cluster)
        internal_pairs = subgraph.number_of_edges()
        internal_weight = sum(d["weight"] for _, _, d in subgraph.edges(data=True))
        avg_weight = internal_weight / internal_pairs if internal_pairs > 0 else 0

        # External edges (pairs connecting to other clusters)
        external_pairs = 0
        for member in members:
            for neighbor in G.neighbors(member):
                if partition[neighbor] != cluster_id:
                    external_pairs += 1
        # Each external edge counted once per endpoint, so divide by...
        # Actually, count from one side only:
        external_pairs_set = set()
        for member in members:
            for neighbor in G.neighbors(member):
                if partition[neighbor] != cluster_id:
                    edge_key = tuple(sorted([member, neighbor]))
                    external_pairs_set.add(edge_key)
        external_pairs = len(external_pairs_set)

        # Check for WhatsApp pod members
        has_whatsapp = any(
            registry.get(m, {}).get("network", "") in WHATSAPP_NETWORKS
            for m in members
        )

        # Hub account: highest degree within cluster
        degrees = dict(subgraph.degree(weight="weight"))
        hub = max(degrees, key=degrees.get) if degrees else ""
        hub_name = registry.get(hub, {}).get("name", hub)

        summaries.append({
            "cluster_id": cluster_id,
            "member_count": len(members),
            "internal_pairs": internal_pairs,
            "external_pairs": external_pairs,
            "avg_weight": round(avg_weight, 1),
            "contains_whatsapp_members": "yes" if has_whatsapp else "no",
            "hub_account": hub,
            "hub_name": hub_name,
        })

        for member in sorted(members):
            info = registry.get(member, {})
            member_rows.append({
                "cluster_id": cluster_id,
                "slug": member,
                "display_name": info.get("name", ""),
                "original_role": info.get("role", "unknown"),
                "cluster_size": len(members),
                "internal_pairs": internal_pairs,
                "external_pairs": external_pairs,
            })

    return summaries, member_rows


def find_bridge_accounts(G, partition, registry):
    """Find profiles with reciprocal pairs in 2+ different clusters."""
    bridges = []

    for node in G.nodes():
        neighbor_clusters = defaultdict(int)
        for neighbor in G.neighbors(node):
            c = partition[neighbor]
            if c != partition[node]:
                neighbor_clusters[c] += 1

        # Include own cluster
        own_cluster = partition[node]
        own_count = sum(1 for n in G.neighbors(node) if partition[n] == own_cluster)
        cluster_counts = {own_cluster: own_count}
        cluster_counts.update(neighbor_clusters)

        clusters_connected = len(cluster_counts)
        if clusters_connected >= 2:
            info = registry.get(node, {})
            bridges.append({
                "slug": node,
                "name": info.get("name", node),
                "role": info.get("role", "unknown"),
                "network": info.get("network", ""),
                "own_cluster": own_cluster,
                "clusters_connected": sorted(cluster_counts.keys()),
                "reciprocal_pairs_per_cluster": {str(k): v for k, v in sorted(cluster_counts.items())},
                "total_reciprocal_pairs": sum(cluster_counts.values()),
                "num_clusters": clusters_connected,
            })

    bridges.sort(key=lambda b: b["num_clusters"], reverse=True)
    return bridges


def find_third_ring_candidates(registry):
    """Find external commenters who should be added to registry."""
    path = ROOT / "data" / "analysis" / "external-commenters.csv"
    registry_slugs = set(registry.keys())
    candidates = []

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = row["commenter_slug"]
            if slug in registry_slugs:
                continue
            if slug == "posts":  # artifact
                continue

            total = int(row["total_comments_on_pod_posts"])
            unique = int(row["unique_pod_members_commented_on"])

            avg_per_member = total / unique if unique > 0 else 0

            # Criteria: 10+ unique members OR (5+ unique AND 15+ avg per member)
            if unique >= 10 or (unique >= 5 and avg_per_member >= 15):
                url = row.get("commenter_linkedin_url", "")
                if not url:
                    url = f"https://www.linkedin.com/in/{slug}"
                candidates.append({
                    "slug": slug,
                    "name": row["commenter_name"],
                    "linkedin_url": url,
                    "total_comments": total,
                    "unique_registry_members": unique,
                    "avg_per_member": round(avg_per_member, 1),
                })

    candidates.sort(key=lambda c: c["unique_registry_members"], reverse=True)
    return candidates


def write_clusters_csv(member_rows, path):
    fields = ["cluster_id", "slug", "display_name", "original_role",
              "cluster_size", "internal_pairs", "external_pairs"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(member_rows)
    print(f"  Saved {len(member_rows)} member assignments to {path}")


def write_cluster_summary(summaries, path):
    fields = ["cluster_id", "member_count", "internal_pairs", "external_pairs",
              "avg_weight", "contains_whatsapp_members", "hub_account", "hub_name"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(summaries)
    print(f"  Saved {len(summaries)} cluster summaries to {path}")


def write_bridge_accounts(bridges, path):
    rows = []
    for b in bridges:
        rows.append({
            "slug": b["slug"],
            "name": b["name"],
            "role": b["role"],
            "clusters_connected": ";".join(str(c) for c in b["clusters_connected"]),
            "reciprocal_pairs_per_cluster": ";".join(
                f"{k}:{v}" for k, v in b["reciprocal_pairs_per_cluster"].items()
            ),
            "total_reciprocal_pairs": b["total_reciprocal_pairs"],
            "num_clusters": b["num_clusters"],
        })
    fields = ["slug", "name", "role", "clusters_connected",
              "reciprocal_pairs_per_cluster", "total_reciprocal_pairs", "num_clusters"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows)} bridge accounts to {path}")


def write_third_ring(candidates, path):
    fields = ["slug", "name", "linkedin_url", "total_comments",
              "unique_registry_members", "avg_per_member"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(candidates)
    print(f"  Saved {len(candidates)} third-ring candidates to {path}")


def print_clusters(summaries, member_rows, registry, partition):
    print("\n" + "=" * 70)
    print("CLUSTER ANALYSIS")
    print("=" * 70)

    for s in summaries:
        cid = s["cluster_id"]
        members = [r for r in member_rows if r["cluster_id"] == cid]
        whatsapp_flag = " [WHATSAPP POD]" if s["contains_whatsapp_members"] == "yes" else ""

        print(f"\n  CLUSTER {cid}{whatsapp_flag}")
        print(f"  Members: {s['member_count']} | Internal pairs: {s['internal_pairs']} | "
              f"External pairs: {s['external_pairs']} | Avg weight: {s['avg_weight']}")
        print(f"  Hub: {s['hub_name']} ({s['hub_account']})")
        print(f"  Members:")
        for m in members:
            name = m["display_name"] or m["slug"]
            role = m["original_role"]
            # Count this member's reciprocal pairs
            print(f"    - {name:<35} ({role})")

    print("\n" + "=" * 70)


def print_bridges(bridges):
    print("\n" + "=" * 70)
    print("BRIDGE ACCOUNTS (connecting 2+ clusters)")
    print("=" * 70)
    print(f"\n  {'Slug':<35} {'Name':<30} {'Clusters':<12} {'Total Pairs'}")
    print(f"  {'='*35} {'='*30} {'='*12} {'='*12}")
    for b in bridges:
        clusters_str = ",".join(str(c) for c in b["clusters_connected"])
        print(f"  {b['slug']:<35} {b['name']:<30} {clusters_str:<12} {b['total_reciprocal_pairs']}")
    print(f"\n  Total bridge accounts: {len(bridges)}")
    print("=" * 70)


def print_third_ring(candidates):
    print("\n" + "=" * 70)
    print("THIRD-RING CANDIDATES (not in registry, high engagement with registry)")
    print("=" * 70)
    print(f"\n  {'#':<4} {'Slug':<35} {'Name':<30} {'Comments':<10} {'Members':<10} {'Avg'}")
    print(f"  {'='*4} {'='*35} {'='*30} {'='*10} {'='*10} {'='*6}")
    for i, c in enumerate(candidates[:30], 1):
        print(f"  {i:<4} {c['slug']:<35} {c['name']:<30} {c['total_comments']:<10} "
              f"{c['unique_registry_members']:<10} {c['avg_per_member']}")
    if len(candidates) > 30:
        print(f"\n  ... and {len(candidates) - 30} more candidates")
    print(f"\n  Total third-ring candidates: {len(candidates)}")
    print("=" * 70)


def main():
    print("Loading registry...")
    registry = load_registry()
    print(f"  {len(registry)} profiles with LinkedIn URLs")

    print("Loading reciprocal pairs...")
    pairs = load_reciprocal_pairs()
    print(f"  {len(pairs)} reciprocal pairs")

    print("\nBuilding network graph...")
    G = build_graph(pairs)
    print(f"  {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    print("\nRunning Louvain community detection...")
    partition = detect_communities(G)
    n_clusters = len(set(partition.values()))
    print(f"  Found {n_clusters} clusters")

    print("\nAnalyzing clusters...")
    summaries, member_rows = analyze_clusters(G, partition, registry)

    analysis_dir = ROOT / "data" / "analysis"
    analysis_dir.mkdir(parents=True, exist_ok=True)

    write_clusters_csv(member_rows, analysis_dir / "clusters.csv")
    write_cluster_summary(summaries, analysis_dir / "cluster-summary.csv")
    print_clusters(summaries, member_rows, registry, partition)

    print("\nFinding bridge accounts...")
    bridges = find_bridge_accounts(G, partition, registry)
    write_bridge_accounts(bridges, analysis_dir / "bridge-accounts.csv")
    print_bridges(bridges)

    print("\nFinding third-ring candidates...")
    candidates = find_third_ring_candidates(registry)
    write_third_ring(candidates, analysis_dir / "third-ring-candidates.csv")
    print_third_ring(candidates)


if __name__ == "__main__":
    main()
