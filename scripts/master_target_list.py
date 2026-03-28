#!/usr/bin/env python3
"""
Master Target List: Combines all analysis outputs into a single prioritized
ring member list for scraping decisions.
"""

import csv
import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CENSUS_FILE = os.path.join(BASE, "data", "analysis", "full-commenter-census.csv")
QUALITY_FILE = os.path.join(BASE, "data", "analysis", "comment-quality.csv")
TIMING_FILE = os.path.join(BASE, "data", "analysis", "comment-timing.csv")
CLUSTERS_FILE = os.path.join(BASE, "data", "analysis", "full-network-clusters.csv")
REGISTRY = os.path.join(BASE, "registry", "profiles.csv")
SCRAPED_DIR = os.path.join(BASE, "data", "scraped")
OUTPUT = os.path.join(BASE, "data", "analysis", "master-ring-members.csv")


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
            }
    return registry


def load_scraped_authors():
    """Get set of already-scraped author slugs."""
    authors = set()
    posts_path = os.path.join(SCRAPED_DIR, "posts.csv")
    if os.path.exists(posts_path):
        with open(posts_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                authors.add(row["author_slug"])
    return authors


def load_census():
    """Load full census."""
    data = {}
    with open(CENSUS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data[row["commenter_slug"]] = row
    return data


def load_quality():
    """Load comment quality data."""
    data = {}
    if os.path.exists(QUALITY_FILE):
        with open(QUALITY_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                data[row["commenter_slug"]] = row
    return data


def load_timing():
    """Load timing data."""
    data = {}
    if os.path.exists(TIMING_FILE):
        with open(TIMING_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                data[row["commenter_slug"]] = row
    return data


def load_clusters():
    """Load cluster assignments."""
    data = {}
    if os.path.exists(CLUSTERS_FILE):
        with open(CLUSTERS_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                data[row["slug"]] = row
    return data


def build_master_list(census, quality, timing, clusters, registry, scraped):
    """Combine all data sources into master list."""
    # Include everyone with 5+ unique authors
    rows = []
    for slug, c in census.items():
        unique_authors = int(c["unique_authors_commented_on"])
        if unique_authors < 5:
            continue

        total = int(c["total_comments"])
        in_reg = slug in registry
        reg_role = registry[slug]["role"] if in_reg else ""
        is_scraped = slug in scraped

        # Quality data
        q = quality.get(slug, {})
        pct_generic = float(q.get("pct_generic", 0))
        suspected_va = q.get("suspected_va", "False") == "True"

        # Timing data
        t = timing.get(slug, {})
        avg_response = float(t.get("avg_response_minutes", 0)) if t else 0

        # Cluster data
        cl = clusters.get(slug, {})
        cluster_id = cl.get("cluster_id", "")

        # Scrape priority
        if is_scraped:
            priority = "scraped"
        elif unique_authors >= 20:
            priority = "critical"
        elif unique_authors >= 10:
            priority = "high"
        elif unique_authors >= 5:
            priority = "medium"
        elif in_reg:
            priority = "registered"
        else:
            priority = "low"

        rows.append({
            "slug": slug,
            "name": c["commenter_name"],
            "linkedin_url": c["linkedin_url"],
            "total_comments_in_dataset": total,
            "unique_authors": unique_authors,
            "in_registry": in_reg,
            "registry_role": reg_role,
            "cluster_id": cluster_id,
            "suspected_va": suspected_va,
            "pct_generic_comments": pct_generic,
            "avg_response_minutes": avg_response,
            "scrape_priority": priority,
        })

    rows.sort(key=lambda r: r["unique_authors"], reverse=True)
    return rows


def write_output(rows):
    """Write master list CSV."""
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    if rows:
        with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    return rows


def display_results(rows):
    """Print priority lists."""
    critical = [r for r in rows if r["scrape_priority"] == "critical"]
    high = [r for r in rows if r["scrape_priority"] == "high"]
    medium = [r for r in rows if r["scrape_priority"] == "medium"]
    scraped = [r for r in rows if r["scrape_priority"] == "scraped"]
    registered = [r for r in rows if r["scrape_priority"] == "registered"]

    print("=" * 110)
    print("MASTER RING MEMBER LIST")
    print("=" * 110)
    print(f"\nTotal accounts (5+ unique authors): {len(rows)}")
    print(f"  Critical (20+ authors, not scraped): {len(critical)}")
    print(f"  High (10-19 authors, not scraped):   {len(high)}")
    print(f"  Medium (5-9 authors, not scraped):   {len(medium)}")
    print(f"  Already scraped:                     {len(scraped)}")
    print(f"  Registered (lower freq):             {len(registered)}")

    if critical:
        print(f"\n{'='*110}")
        print(f"CRITICAL PRIORITY - Scrape first when credits renew ({len(critical)} accounts)")
        print(f"{'='*110}")
        print(f"{'Slug':<40} {'Authors':>7} {'Comments':>8} {'%Generic':>8} {'AvgMin':>7} {'VA?':>4} {'Cluster':>7} {'Registry'}")
        print("-" * 110)
        for r in critical:
            reg = "YES" if r["in_registry"] else "---"
            va = "YES" if r["suspected_va"] else ""
            cluster = r["cluster_id"] if r["cluster_id"] else "-"
            print(f"{r['slug']:<40} {r['unique_authors']:>7} {r['total_comments_in_dataset']:>8} {r['pct_generic_comments']:>7.1f}% {r['avg_response_minutes']:>7.0f} {va:>4} {cluster:>7} {reg}")

    if high:
        print(f"\n{'='*110}")
        print(f"HIGH PRIORITY - Scrape second ({len(high)} accounts)")
        print(f"{'='*110}")
        print(f"{'Slug':<40} {'Authors':>7} {'Comments':>8} {'%Generic':>8} {'AvgMin':>7} {'VA?':>4} {'Cluster':>7} {'Registry'}")
        print("-" * 110)
        for r in high:
            reg = "YES" if r["in_registry"] else "---"
            va = "YES" if r["suspected_va"] else ""
            cluster = r["cluster_id"] if r["cluster_id"] else "-"
            print(f"{r['slug']:<40} {r['unique_authors']:>7} {r['total_comments_in_dataset']:>8} {r['pct_generic_comments']:>7.1f}% {r['avg_response_minutes']:>7.0f} {va:>4} {cluster:>7} {reg}")


if __name__ == "__main__":
    print("Loading all data sources...")
    registry = load_registry()
    scraped = load_scraped_authors()
    census = load_census()
    quality = load_quality()
    timing = load_timing()
    clusters = load_clusters()

    print(f"Registry: {len(registry)}, Scraped: {len(scraped)}, Census: {len(census)}")
    print(f"Quality: {len(quality)}, Timing: {len(timing)}, Clusters: {len(clusters)}")

    print("Building master list...")
    rows = build_master_list(census, quality, timing, clusters, registry, scraped)

    print("Writing output...")
    rows = write_output(rows)

    display_results(rows)
    print(f"\nOutput: {OUTPUT}")
