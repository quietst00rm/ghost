#!/usr/bin/env python3
"""
Full Commenter Census: Analyze every commenter across all scraped data.
Identifies ring members by cross-profile commenting frequency.
"""

import csv
import os
from collections import defaultdict
from datetime import datetime

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPED_DIR = os.path.join(BASE, "data", "scraped")
REGISTRY = os.path.join(BASE, "registry", "profiles.csv")
OUTPUT = os.path.join(BASE, "data", "analysis", "full-commenter-census.csv")


def load_registry():
    """Load registry profiles, return set of slugs and dict of slug -> profile_id/role."""
    registry = {}
    with open(REGISTRY, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = row["linkedin_url"].rstrip("/").split("?")[0]
            slug = url.split("/")[-1]
            registry[slug] = {
                "profile_id": row["profile_id"],
                "role": row["role"],
                "display_name": row["display_name"],
            }
    return registry


def load_comments():
    """Load all comments from comments.csv."""
    comments = []
    path = os.path.join(SCRAPED_DIR, "comments.csv")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            comments.append(row)
    return comments


def build_census(comments):
    """Build commenter census from all comments."""
    # commenter_slug -> tracking dict
    census = defaultdict(lambda: {
        "name": "",
        "linkedin_url": "",
        "authors": defaultdict(int),  # author_slug -> count
        "dates": [],
        "total": 0,
    })

    for c in comments:
        slug = c["comment_author_slug"]
        if not slug:
            continue

        entry = census[slug]
        entry["name"] = c["comment_author_name"]
        entry["linkedin_url"] = c["comment_author_linkedin_url"]
        entry["authors"][c["post_author_slug"]] += 1
        entry["total"] += 1

        ts = c.get("comment_timestamp", "")
        if ts:
            entry["dates"].append(ts)

    return census


def write_output(census, registry):
    """Write census CSV sorted by unique_authors descending."""
    rows = []
    for slug, data in census.items():
        unique_authors = len(data["authors"])
        dates = sorted(data["dates"])
        earliest = dates[0] if dates else ""
        latest = dates[-1] if dates else ""
        author_list = ";".join(sorted(data["authors"].keys()))
        avg_per_author = round(data["total"] / unique_authors, 2) if unique_authors else 0

        in_registry = slug in registry
        registry_id = registry[slug]["profile_id"] if in_registry else ""
        registry_role = registry[slug]["role"] if in_registry else ""

        rows.append({
            "commenter_slug": slug,
            "commenter_name": data["name"],
            "linkedin_url": data["linkedin_url"],
            "total_comments": data["total"],
            "unique_authors_commented_on": unique_authors,
            "avg_comments_per_author": avg_per_author,
            "earliest_comment": earliest,
            "latest_comment": latest,
            "in_registry": in_registry,
            "registry_id": registry_id,
            "registry_role": registry_role,
            "authors_commented_on": author_list,
        })

    rows.sort(key=lambda r: r["unique_authors_commented_on"], reverse=True)

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return rows


def display_results(rows, registry):
    """Print summary statistics."""
    total = len(rows)
    hit_10 = [r for r in rows if r["unique_authors_commented_on"] >= 10]
    hit_20 = [r for r in rows if r["unique_authors_commented_on"] >= 20]
    hit_30 = [r for r in rows if r["unique_authors_commented_on"] >= 30]

    not_registered_10 = [r for r in hit_10 if not r["in_registry"]]
    not_registered_20 = [r for r in hit_20 if not r["in_registry"]]
    not_registered_30 = [r for r in hit_30 if not r["in_registry"]]

    print("=" * 80)
    print("FULL COMMENTER CENSUS")
    print("=" * 80)
    print(f"\nTotal unique commenters across all scraped data: {total:,}")
    print(f"\nCommenters by reach:")
    print(f"  10+ unique authors (the ring):     {len(hit_10):>4}  ({len(not_registered_10)} NOT in registry)")
    print(f"  20+ unique authors (inner ring):   {len(hit_20):>4}  ({len(not_registered_20)} NOT in registry)")
    print(f"  30+ unique authors (hub accounts): {len(hit_30):>4}  ({len(not_registered_30)} NOT in registry)")

    print(f"\n{'='*80}")
    print("TOP 50 COMMENTERS BY UNIQUE AUTHORS")
    print(f"{'='*80}")
    print(f"{'Slug':<40} {'Authors':>7} {'Comments':>8} {'Avg/Auth':>8} {'Registry':>10}")
    print("-" * 80)
    for r in rows[:50]:
        reg = r["registry_id"] if r["in_registry"] else "---"
        print(f"{r['commenter_slug']:<40} {r['unique_authors_commented_on']:>7} {r['total_comments']:>8} {r['avg_comments_per_author']:>8} {reg:>10}")

    if not_registered_10:
        print(f"\n{'='*80}")
        print(f"UNREGISTERED RING MEMBERS (10+ authors, NOT in registry): {len(not_registered_10)}")
        print(f"{'='*80}")
        print(f"{'Slug':<40} {'Authors':>7} {'Comments':>8} {'Name'}")
        print("-" * 80)
        for r in sorted(not_registered_10, key=lambda x: x["unique_authors_commented_on"], reverse=True):
            print(f"{r['commenter_slug']:<40} {r['unique_authors_commented_on']:>7} {r['total_comments']:>8} {r['commenter_name']}")


if __name__ == "__main__":
    print("Loading registry...")
    registry = load_registry()
    print(f"Registry: {len(registry)} profiles")

    print("Loading comments...")
    comments = load_comments()
    print(f"Comments: {len(comments):,}")

    print("Building census...")
    census = build_census(comments)

    print("Writing output...")
    rows = write_output(census, registry)

    display_results(rows, registry)
    print(f"\nOutput: {OUTPUT}")
