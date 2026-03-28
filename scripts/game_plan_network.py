#!/usr/bin/env python3
"""Task 1: Map The Game Plan's client network from existing scraped data."""

import csv
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMENTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "comments.csv")
PROFILES_CSV = os.path.join(BASE_DIR, "registry", "profiles.csv")
CENSUS_CSV = os.path.join(BASE_DIR, "data", "analysis", "full-commenter-census.csv")
OUT_TARGETS = os.path.join(BASE_DIR, "data", "analysis", "game-plan-targets.csv")
OUT_COCOMMENTERS = os.path.join(BASE_DIR, "data", "analysis", "game-plan-co-commenters.csv")

GAME_PLAN_SLUG = "posts"


def load_registry():
    reg = {}
    with open(PROFILES_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug = row["linkedin_url"].rstrip("/").split("/")[-1]
            reg[slug] = row
    return reg


def load_comments():
    comments = []
    with open(COMMENTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            comments.append(row)
    return comments


def load_census():
    census = {}
    with open(CENSUS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            census[row["commenter_slug"]] = row
    return census


def main():
    print("Loading data...")
    comments = load_comments()
    registry = load_registry()
    census = load_census()

    print(f"  {len(comments)} comments, {len(registry)} registry profiles, {len(census)} census entries")

    # Step 1: Extract every author The Game Plan commented on
    gp_comments = [c for c in comments if c["comment_author_slug"] == GAME_PLAN_SLUG]
    print(f"\nThe Game Plan total comments: {len(gp_comments)}")

    # Group by author
    by_author = defaultdict(list)
    for c in gp_comments:
        by_author[c["post_author_slug"]].append(c)

    print(f"The Game Plan comments on {len(by_author)} unique authors")

    # Step 2: Build game-plan-targets.csv
    targets = []
    for author_slug, author_comments in sorted(by_author.items(), key=lambda x: -len(x[1])):
        author_comments_sorted = sorted(author_comments, key=lambda x: x["comment_timestamp"])
        sample = [c["comment_text"][:200] for c in author_comments_sorted[:3]]
        # Find author name from posts or comments
        author_name = ""
        for c in author_comments:
            # Get from any comment on that post
            pass
        # Try to find author name from all comments
        for c in comments:
            if c["post_author_slug"] == author_slug:
                # The post author name isn't in comments.csv, check if they commented on someone
                pass
                break
        # Check census for name
        if author_slug in census:
            author_name = census[author_slug].get("commenter_name", "")
        # Check registry
        in_reg = author_slug in registry or any(
            row["linkedin_url"].rstrip("/").split("/")[-1] == author_slug
            for row in registry.values()
        )
        reg_role = registry.get(author_slug, {}).get("role", "")
        if not author_name and author_slug in registry:
            author_name = registry[author_slug].get("display_name", "")

        targets.append({
            "author_slug": author_slug,
            "author_name": author_name,
            "comment_count": len(author_comments),
            "earliest_comment": author_comments_sorted[0]["comment_timestamp"],
            "latest_comment": author_comments_sorted[-1]["comment_timestamp"],
            "in_registry": in_reg,
            "registry_role": reg_role,
            "sample_comments": " ||| ".join(sample),
        })

    # Try to get author names from posts.csv
    posts_csv = os.path.join(BASE_DIR, "data", "scraped", "posts.csv")
    author_names = {}
    with open(posts_csv, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            author_names[row["author_slug"]] = row["author_name"]

    # Also get names from census
    for slug, cdata in census.items():
        if slug not in author_names:
            author_names[slug] = cdata.get("commenter_name", "")

    for t in targets:
        if not t["author_name"] and t["author_slug"] in author_names:
            t["author_name"] = author_names[t["author_slug"]]

    # Write targets CSV
    with open(OUT_TARGETS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "author_slug", "author_name", "comment_count", "earliest_comment",
            "latest_comment", "in_registry", "registry_role", "sample_comments"
        ])
        writer.writeheader()
        writer.writerows(targets)

    print(f"\nWrote {len(targets)} targets to {OUT_TARGETS}")

    # Step 3: For each of the 68 authors, find co-commenters (5+ comments) who also comment on that author
    # Build a map: author_slug -> set of commenters with 5+ comments on them
    print("\nBuilding author->commenter index for co-commenter analysis...")
    author_commenters = defaultdict(lambda: defaultdict(int))
    for c in comments:
        author_commenters[c["post_author_slug"]][c["comment_author_slug"]] += 1

    gp_target_slugs = set(by_author.keys())

    # For each GP target, find commenters with 5+ comments (excluding GP itself)
    co_commenter_rows = []
    co_commenter_overlap = defaultdict(lambda: defaultdict(int))  # co_commenter -> {author -> count}

    for target_slug in gp_target_slugs:
        for commenter_slug, count in author_commenters[target_slug].items():
            if commenter_slug == GAME_PLAN_SLUG:
                continue
            if count >= 5:
                co_commenter_overlap[commenter_slug][target_slug] = count

    # Build co-commenter CSV: for each (target, co_commenter), total_authors_in_common_with_game_plan
    for commenter_slug, target_counts in co_commenter_overlap.items():
        total_common = len(target_counts)
        commenter_name = author_names.get(commenter_slug, census.get(commenter_slug, {}).get("commenter_name", ""))
        for target_slug, count in target_counts.items():
            co_commenter_rows.append({
                "author_slug": target_slug,
                "co_commenter_slug": commenter_slug,
                "co_commenter_name": commenter_name,
                "comments_on_this_author": count,
                "total_authors_in_common_with_game_plan": total_common,
            })

    co_commenter_rows.sort(key=lambda x: (-x["total_authors_in_common_with_game_plan"], -x["comments_on_this_author"]))

    with open(OUT_COCOMMENTERS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "author_slug", "co_commenter_slug", "co_commenter_name",
            "comments_on_this_author", "total_authors_in_common_with_game_plan"
        ])
        writer.writeheader()
        writer.writerows(co_commenter_rows)

    print(f"Wrote {len(co_commenter_rows)} co-commenter rows to {OUT_COCOMMENTERS}")

    # Display: 68 targets sorted by Game Plan comment count
    print("\n" + "=" * 80)
    print("THE GAME PLAN'S 68 TARGETS (sorted by comment count)")
    print("=" * 80)
    print(f"{'#':<4} {'Slug':<40} {'Name':<30} {'Count':<6} {'Registry':<10} {'Role':<12}")
    print("-" * 102)
    for i, t in enumerate(targets, 1):
        name = t["author_name"][:28] if t["author_name"] else ""
        print(f"{i:<4} {t['author_slug']:<40} {name:<30} {t['comment_count']:<6} "
              f"{'Yes' if t['in_registry'] else 'No':<10} {t['registry_role']:<12}")

    # Top 30 co-commenters by overlap count
    print("\n" + "=" * 80)
    print("TOP 30 CO-COMMENTERS (most targets in common with The Game Plan)")
    print("=" * 80)
    # Aggregate: unique co-commenters sorted by total_authors_in_common
    co_commenter_summary = {}
    for row in co_commenter_rows:
        slug = row["co_commenter_slug"]
        if slug not in co_commenter_summary:
            co_commenter_summary[slug] = {
                "slug": slug,
                "name": row["co_commenter_name"],
                "targets_in_common": row["total_authors_in_common_with_game_plan"],
                "total_comments_on_targets": 0,
            }
        co_commenter_summary[slug]["total_comments_on_targets"] += row["comments_on_this_author"]

    sorted_cc = sorted(co_commenter_summary.values(), key=lambda x: -x["targets_in_common"])

    print(f"{'#':<4} {'Slug':<40} {'Name':<30} {'Targets':<8} {'Comments':<10}")
    print("-" * 92)
    for i, cc in enumerate(sorted_cc[:30], 1):
        name = cc["name"][:28] if cc["name"] else ""
        print(f"{i:<4} {cc['slug']:<40} {name:<30} {cc['targets_in_common']:<8} {cc['total_comments_on_targets']:<10}")

    # Co-commenters on 20+ targets
    print("\n" + "=" * 80)
    print("CO-COMMENTERS ON 20+ OF THE GAME PLAN'S 68 TARGETS")
    print("(Likely VAs or core pod members operating at the same level)")
    print("=" * 80)
    heavy_cc = [cc for cc in sorted_cc if cc["targets_in_common"] >= 20]
    print(f"{'#':<4} {'Slug':<40} {'Name':<30} {'Targets':<8} {'Comments':<10}")
    print("-" * 92)
    for i, cc in enumerate(heavy_cc, 1):
        name = cc["name"][:28] if cc["name"] else ""
        print(f"{i:<4} {cc['slug']:<40} {name:<30} {cc['targets_in_common']:<8} {cc['total_comments_on_targets']:<10}")

    print(f"\nTotal co-commenters on 20+ targets: {len(heavy_cc)}")
    print(f"Total unique co-commenters (5+ comments on any GP target): {len(co_commenter_summary)}")


if __name__ == "__main__":
    main()
