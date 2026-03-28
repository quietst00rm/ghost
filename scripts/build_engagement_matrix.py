#!/usr/bin/env python3
"""
build_engagement_matrix.py — Build engagement matrix from scraped LinkedIn comments.

Reads data/scraped/comments.csv and registry/profiles.csv.
Outputs:
    data/analysis/engagement-matrix.csv  — Square commenter-vs-author matrix
    data/analysis/top-pairs.csv          — All commenter-author pairs sorted by count
    data/analysis/external-commenters.csv — Non-registry commenters on pod members

Usage:
    python scripts/build_engagement_matrix.py
"""

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


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
                "profile_id": row.get("profile_id", ""),
                "linkedin_url": url,
            }
    return profiles


def load_comments():
    """Load comments.csv."""
    path = ROOT / "data" / "scraped" / "comments.csv"
    if not path.exists():
        print(f"ERROR: {path} not found. Run scrape_linkedin.py first.")
        sys.exit(1)
    comments = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            comments.append(row)
    return comments


def build_matrix(comments, registry):
    """Build commenter->author->count mapping for registry members only."""
    registry_slugs = set(registry.keys())
    pair_counts = Counter()

    for c in comments:
        commenter = c.get("comment_author_slug", "").lower()
        author = c.get("post_author_slug", "").lower()
        if commenter and author and commenter in registry_slugs and author in registry_slugs:
            if commenter != author:  # exclude self-comments
                pair_counts[(commenter, author)] += 1

    return pair_counts


def write_engagement_matrix(pair_counts, registry, output_path):
    """Write square matrix CSV: rows=commenters, cols=authors."""
    # Collect all slugs that appear in pairs
    all_slugs = set()
    for commenter, author in pair_counts:
        all_slugs.add(commenter)
        all_slugs.add(author)
    all_slugs = sorted(all_slugs)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["commenter_slug"] + all_slugs)
        for commenter in all_slugs:
            row = [commenter]
            for author in all_slugs:
                row.append(pair_counts.get((commenter, author), 0))
            writer.writerow(row)

    print(f"  Saved engagement matrix ({len(all_slugs)}x{len(all_slugs)}) to {output_path}")


def write_top_pairs(pair_counts, registry, output_path):
    """Write top pairs CSV sorted by comment count descending."""
    rows = []
    for (commenter, author), count in pair_counts.most_common():
        rows.append({
            "commenter_slug": commenter,
            "author_slug": author,
            "comment_count": count,
            "commenter_name": registry.get(commenter, {}).get("name", ""),
            "author_name": registry.get(author, {}).get("name", ""),
        })

    fields = ["commenter_slug", "author_slug", "comment_count", "commenter_name", "author_name"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Saved {len(rows)} pairs to {output_path}")


def find_external_commenters(comments, registry):
    """Find commenters NOT in registry who comment on registry members' posts."""
    registry_slugs = set(registry.keys())
    external = defaultdict(lambda: {
        "name": "",
        "url": "",
        "total_comments": 0,
        "authors": set(),
    })

    for c in comments:
        commenter = c.get("comment_author_slug", "").lower()
        author = c.get("post_author_slug", "").lower()
        if not commenter or not author:
            continue
        if commenter in registry_slugs:
            continue
        if author not in registry_slugs:
            continue

        ext = external[commenter]
        if not ext["name"]:
            ext["name"] = c.get("comment_author_name", "")
        if not ext["url"]:
            ext["url"] = c.get("comment_author_linkedin_url", "")
        ext["total_comments"] += 1
        ext["authors"].add(author)

    return external


def write_external_commenters(external, output_path):
    """Write external commenters CSV, sorted by unique pod members commented on."""
    rows = []
    for slug, data in external.items():
        rows.append({
            "commenter_slug": slug,
            "commenter_name": data["name"],
            "commenter_linkedin_url": data["url"],
            "total_comments_on_pod_posts": data["total_comments"],
            "unique_pod_members_commented_on": len(data["authors"]),
        })

    rows.sort(key=lambda r: r["unique_pod_members_commented_on"], reverse=True)

    fields = [
        "commenter_slug", "commenter_name", "commenter_linkedin_url",
        "total_comments_on_pod_posts", "unique_pod_members_commented_on",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    flagged = [r for r in rows if r["unique_pod_members_commented_on"] >= 5]
    print(f"  Saved {len(rows)} external commenters to {output_path}")
    print(f"  Flagged (5+ unique pod members): {len(flagged)}")

    return rows, flagged


def print_summary(pair_counts, comments, registry, external_rows, flagged):
    """Print analysis summary."""
    print("\n" + "=" * 60)
    print("ENGAGEMENT MATRIX SUMMARY")
    print("=" * 60)

    total_comments = len(comments)
    registry_slugs = set(registry.keys())
    unique_commenters = set()
    for c in comments:
        slug = c.get("comment_author_slug", "").lower()
        if slug:
            unique_commenters.add(slug)

    registry_commenters = unique_commenters & registry_slugs
    total_registry_comments = sum(pair_counts.values())

    print(f"  Total comments in dataset:    {total_comments:,}")
    print(f"  Unique commenters (all):      {len(unique_commenters):,}")
    print(f"  Registry commenters:          {len(registry_commenters):,}")
    print(f"  Registry-to-registry comments:{total_registry_comments:,}")

    if pair_counts:
        avg = total_registry_comments / len(pair_counts) if pair_counts else 0
        print(f"  Unique pairs (registry):      {len(pair_counts):,}")
        print(f"  Average comments per pair:    {avg:.1f}")

    print(f"\n  TOP 20 COMMENTER-AUTHOR PAIRS:")
    print(f"  {'#':<4} {'Count':<7} {'Commenter':<30} {'Author':<30}")
    print(f"  {'—'*4} {'—'*7} {'—'*30} {'—'*30}")
    for i, ((commenter, author), count) in enumerate(pair_counts.most_common(20), 1):
        c_name = registry.get(commenter, {}).get("name", commenter)
        a_name = registry.get(author, {}).get("name", author)
        print(f"  {i:<4} {count:<7} {c_name:<30} {a_name:<30}")

    if flagged:
        print(f"\n  FLAGGED EXTERNAL COMMENTERS (5+ unique pod members):")
        print(f"  {'Slug':<30} {'Name':<25} {'Comments':<10} {'Unique Members'}")
        print(f"  {'—'*30} {'—'*25} {'—'*10} {'—'*15}")
        for r in flagged[:20]:
            print(f"  {r['commenter_slug']:<30} {r['commenter_name']:<25} {r['total_comments_on_pod_posts']:<10} {r['unique_pod_members_commented_on']}")

    print("=" * 60)


def main():
    print("Loading registry...")
    registry = load_registry()
    print(f"  {len(registry)} profiles with LinkedIn URLs")

    print("Loading comments...")
    comments = load_comments()
    print(f"  {len(comments)} comments loaded")

    print("\nBuilding engagement matrix...")
    pair_counts = build_matrix(comments, registry)

    analysis_dir = ROOT / "data" / "analysis"

    write_engagement_matrix(pair_counts, registry, analysis_dir / "engagement-matrix.csv")
    write_top_pairs(pair_counts, registry, analysis_dir / "top-pairs.csv")

    print("\nFinding external commenters...")
    external = find_external_commenters(comments, registry)
    external_rows, flagged = write_external_commenters(external, analysis_dir / "external-commenters.csv")

    print_summary(pair_counts, comments, registry, external_rows, flagged)


if __name__ == "__main__":
    main()
