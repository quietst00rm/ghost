#!/usr/bin/env python3
"""Tasks 2-4: Deep analysis of Victor Trieu, Charlie Hills, Chris Lang."""

import csv
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMENTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "comments.csv")
POSTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "posts.csv")
PROFILES_CSV = os.path.join(BASE_DIR, "registry", "profiles.csv")
CENSUS_CSV = os.path.join(BASE_DIR, "data", "analysis", "full-commenter-census.csv")
GP_TARGETS_CSV = os.path.join(BASE_DIR, "data", "analysis", "game-plan-targets.csv")

TARGETS = {
    "victortrieu": {
        "name": "Victor Trieu",
        "output": os.path.join(BASE_DIR, "data", "analysis", "victor-trieu-commenters.csv"),
    },
    "charlie-hills": {
        "name": "Charlie Hills",
        "output": os.path.join(BASE_DIR, "data", "analysis", "charlie-hills-commenters.csv"),
    },
    "chrislangsocial": {
        "name": "Chris Lang",
        "output": os.path.join(BASE_DIR, "data", "analysis", "chris-lang-commenters.csv"),
    },
}

KEY_SLUGS = {
    "shane": "shanebarker",
    "cory": "coryblumenfeld",
    "charlie": "charlie-hills",
    "victor": "victortrieu",
}


def load_registry():
    reg = {}
    with open(PROFILES_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug = row["linkedin_url"].rstrip("/").split("/")[-1]
            reg[slug] = row
    return reg


def load_gp_targets():
    targets = set()
    with open(GP_TARGETS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            targets.add(row["author_slug"])
    return targets


def main():
    print("Loading data...")
    comments = []
    with open(COMMENTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            comments.append(row)

    registry = load_registry()
    gp_targets = load_gp_targets()

    # Build author names lookup from posts
    author_names = {}
    with open(POSTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            author_names[row["author_slug"]] = row["author_name"]

    # Build indexes
    # author -> {commenter -> [comments]}
    author_commenters = defaultdict(lambda: defaultdict(list))
    # commenter -> set of authors they comment on
    commenter_authors = defaultdict(set)
    # commenter -> name and url
    commenter_info = {}

    for c in comments:
        author_commenters[c["post_author_slug"]][c["comment_author_slug"]].append(c)
        commenter_authors[c["comment_author_slug"]].add(c["post_author_slug"])
        if c["comment_author_slug"] not in commenter_info:
            commenter_info[c["comment_author_slug"]] = {
                "name": c["comment_author_name"],
                "url": c["comment_author_linkedin_url"],
            }

    print(f"  {len(comments)} comments loaded")

    for target_slug, target_info in TARGETS.items():
        print(f"\n{'=' * 80}")
        print(f"DEEP ANALYSIS: {target_info['name']} ({target_slug})")
        print(f"{'=' * 80}")

        target_comments = author_commenters[target_slug]
        total_comments = sum(len(clist) for clist in target_comments.values())
        unique_commenters = len(target_comments)

        print(f"Total comments on posts: {total_comments}")
        print(f"Unique commenters: {unique_commenters}")

        # Count posts
        post_ids = set()
        for clist in target_comments.values():
            for c in clist:
                post_ids.add(c["post_id"])
        print(f"Posts with comments: {len(post_ids)}")

        # Build commenter analysis
        rows = []
        for commenter_slug, clist in target_comments.items():
            if commenter_slug == target_slug:
                continue  # skip self-comments

            info = commenter_info.get(commenter_slug, {})
            name = info.get("name", "")
            url = info.get("url", "")
            comment_count = len(clist)
            posts_on = len(set(c["post_id"] for c in clist))

            # Check overlap with key profiles
            also_shane = commenter_slug in author_commenters.get(KEY_SLUGS["shane"], {})
            also_cory = commenter_slug in author_commenters.get(KEY_SLUGS["cory"], {})
            also_charlie = commenter_slug in author_commenters.get(KEY_SLUGS["charlie"], {})

            in_reg = commenter_slug in registry
            reg_role = registry.get(commenter_slug, {}).get("role", "")

            # Total ring overlap = how many of the 68 GP targets this commenter also comments on
            ring_overlap = len(commenter_authors[commenter_slug] & gp_targets)

            rows.append({
                "commenter_slug": commenter_slug,
                "name": name,
                "linkedin_url": url,
                "comment_count": comment_count,
                "posts_commented_on": posts_on,
                "also_comments_on_shane": also_shane,
                "also_comments_on_cory": also_cory,
                "also_comments_on_charlie": also_charlie,
                "in_registry": in_reg,
                "total_ring_overlap": ring_overlap,
            })

        rows.sort(key=lambda x: -x["comment_count"])

        # Write CSV
        with open(target_info["output"], "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "commenter_slug", "name", "linkedin_url", "comment_count",
                "posts_commented_on", "also_comments_on_shane", "also_comments_on_cory",
                "also_comments_on_charlie", "in_registry", "total_ring_overlap"
            ])
            writer.writeheader()
            writer.writerows(rows)

        print(f"\nWrote {len(rows)} commenters to {target_info['output']}")

        # Display top 40
        print(f"\nTOP 40 COMMENTERS ON {target_info['name'].upper()}")
        print(f"{'#':<4} {'Slug':<38} {'Name':<28} {'Cnt':<5} {'Posts':<6} {'Shane':<6} {'Cory':<6} {'Char':<6} {'Reg':<5} {'Overlap':<8}")
        print("-" * 112)
        for i, r in enumerate(rows[:40], 1):
            name = r["name"][:26] if r["name"] else ""
            print(f"{i:<4} {r['commenter_slug']:<38} {name:<28} {r['comment_count']:<5} "
                  f"{r['posts_commented_on']:<6} {'Y' if r['also_comments_on_shane'] else '-':<6} "
                  f"{'Y' if r['also_comments_on_cory'] else '-':<6} "
                  f"{'Y' if r['also_comments_on_charlie'] else '-':<6} "
                  f"{'Y' if r['in_registry'] else '-':<5} {r['total_ring_overlap']:<8}")

        # Calculate overlap percentages
        total_with_comments = len(rows)
        shane_overlap = sum(1 for r in rows if r["also_comments_on_shane"])
        cory_overlap = sum(1 for r in rows if r["also_comments_on_cory"])
        charlie_overlap = sum(1 for r in rows if r["also_comments_on_charlie"])

        print(f"\nOVERLAP ANALYSIS:")
        print(f"  Commenters also on Shane Barker: {shane_overlap}/{total_with_comments} ({100*shane_overlap/total_with_comments:.1f}%)")
        print(f"  Commenters also on Cory Blumenfeld: {cory_overlap}/{total_with_comments} ({100*cory_overlap/total_with_comments:.1f}%)")
        print(f"  Commenters also on Charlie Hills: {charlie_overlap}/{total_with_comments} ({100*charlie_overlap/total_with_comments:.1f}%)")

        # For Charlie Hills: identify 10+ commenters NOT in registry
        if target_slug == "charlie-hills":
            print(f"\nUNREGISTERED COMMENTERS ON CHARLIE HILLS (10+ comments):")
            unreg = [r for r in rows if not r["in_registry"] and r["comment_count"] >= 10]
            print(f"{'#':<4} {'Slug':<40} {'Name':<30} {'Count':<6} {'Ring Overlap':<12}")
            print("-" * 92)
            for i, r in enumerate(unreg, 1):
                name = r["name"][:28] if r["name"] else ""
                print(f"{i:<4} {r['commenter_slug']:<40} {name:<30} {r['comment_count']:<6} {r['total_ring_overlap']:<12}")

        # For Victor Trieu: also show overlap with other analysis targets
        if target_slug == "victortrieu":
            victor_overlap = sum(1 for r in rows if r["also_comments_on_shane"] and r["also_comments_on_cory"])
            print(f"  Commenters on BOTH Shane AND Cory: {victor_overlap}/{total_with_comments} ({100*victor_overlap/total_with_comments:.1f}%)")

        # For Chris Lang: check overlap with Victor Trieu
        if target_slug == "chrislangsocial":
            victor_commenters = set(author_commenters.get("victortrieu", {}).keys())
            chris_commenters = set(target_comments.keys())
            overlap_victor = len(chris_commenters & victor_commenters)
            print(f"  Commenters also on Victor Trieu: {overlap_victor}/{total_with_comments} ({100*overlap_victor/total_with_comments:.1f}%)")

    # Check if Victor Trieu is in registry
    print(f"\n{'=' * 80}")
    print("REGISTRY CHECKS")
    print(f"{'=' * 80}")
    for slug in ["victortrieu", "chrislangsocial"]:
        if slug in registry:
            r = registry[slug]
            print(f"  {slug}: {r['display_name']} - role={r['role']}, confidence={r['confidence']}, network={r['network']}")
        else:
            print(f"  {slug}: NOT IN REGISTRY")


if __name__ == "__main__":
    main()
