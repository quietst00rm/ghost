#!/usr/bin/env python3
"""Task 6: Operator footprint analysis - Cory and Charlie's engagement behavior."""

import csv
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMENTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "comments.csv")
POSTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "posts.csv")
PROFILES_CSV = os.path.join(BASE_DIR, "registry", "profiles.csv")
GP_TARGETS_CSV = os.path.join(BASE_DIR, "data", "analysis", "game-plan-targets.csv")
OUT_FOOTPRINT = os.path.join(BASE_DIR, "data", "analysis", "operator-footprint.csv")
OUT_SHARED = os.path.join(BASE_DIR, "data", "analysis", "operator-shared-targets.csv")

OPERATORS = {
    "cory": "coryblumenfeld",
    "charlie": "charlie-hills",
}


def load_registry():
    reg = {}
    with open(PROFILES_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug = row["linkedin_url"].rstrip("/").split("/")[-1]
            reg[slug] = row
    return reg


def main():
    print("Loading data...")
    comments = []
    with open(COMMENTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            comments.append(row)

    registry = load_registry()

    # Load GP target comment counts
    gp_comments = {}
    with open(GP_TARGETS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            gp_comments[row["author_slug"]] = int(row["comment_count"])

    # Author names from posts
    author_names = {}
    with open(POSTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            author_names[row["author_slug"]] = row["author_name"]

    # Also get names from comments
    for c in comments:
        if c["comment_author_slug"] not in author_names:
            author_names[c["comment_author_slug"]] = c["comment_author_name"]
        if c["post_author_slug"] not in author_names:
            author_names[c["post_author_slug"]] = ""

    # Build indexes
    # who does X comment on? (outgoing)
    outgoing = defaultdict(lambda: defaultdict(int))
    # who comments on X? (incoming)
    incoming = defaultdict(lambda: defaultdict(int))

    for c in comments:
        outgoing[c["comment_author_slug"]][c["post_author_slug"]] += 1
        incoming[c["post_author_slug"]][c["comment_author_slug"]] += 1

    # Count ring members commenting on each target
    ring_member_slugs = set(registry.keys())

    # Part A: Operator footprint CSV
    footprint_rows = []

    for op_label, op_slug in OPERATORS.items():
        # Profiles the operator comments on (outgoing)
        for target, count in sorted(outgoing[op_slug].items(), key=lambda x: -x[1]):
            role = registry.get(target, {}).get("role", "")
            ring_commenters = sum(1 for s in incoming[target] if s in ring_member_slugs)
            name = author_names.get(target, "")
            footprint_rows.append({
                "operator": op_label,
                "direction": "outgoing",
                "slug": target,
                "name": name,
                "comment_count": count,
                "role": role,
                "ring_members_also_engaging": ring_commenters,
            })

        # Profiles that comment on the operator (incoming)
        for commenter, count in sorted(incoming[op_slug].items(), key=lambda x: -x[1]):
            role = registry.get(commenter, {}).get("role", "")
            name = author_names.get(commenter, "")
            footprint_rows.append({
                "operator": op_label,
                "direction": "incoming",
                "slug": commenter,
                "name": name,
                "comment_count": count,
                "role": role,
                "ring_members_also_engaging": 0,
            })

    with open(OUT_FOOTPRINT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "operator", "direction", "slug", "name", "comment_count",
            "role", "ring_members_also_engaging"
        ])
        writer.writeheader()
        writer.writerows(footprint_rows)

    print(f"Wrote {len(footprint_rows)} footprint rows to {OUT_FOOTPRINT}")

    # Part B: Shared targets - profiles BOTH Cory and Charlie comment on
    cory_targets = outgoing[OPERATORS["cory"]]
    charlie_targets = outgoing[OPERATORS["charlie"]]

    shared_slugs = set(cory_targets.keys()) & set(charlie_targets.keys())

    shared_rows = []
    for slug in shared_slugs:
        name = author_names.get(slug, "")
        cory_count = cory_targets[slug]
        charlie_count = charlie_targets[slug]
        combined = cory_count + charlie_count
        gp_count = gp_comments.get(slug, 0)
        in_reg = slug in registry
        role = registry.get(slug, {}).get("role", "")

        shared_rows.append({
            "target_slug": slug,
            "target_name": name,
            "cory_comments": cory_count,
            "charlie_comments": charlie_count,
            "combined": combined,
            "game_plan_comments": gp_count,
            "in_registry": in_reg,
            "role": role,
        })

    shared_rows.sort(key=lambda x: -x["combined"])

    with open(OUT_SHARED, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "target_slug", "target_name", "cory_comments", "charlie_comments",
            "combined", "game_plan_comments", "in_registry", "role"
        ])
        writer.writeheader()
        writer.writerows(shared_rows)

    print(f"Wrote {len(shared_rows)} shared targets to {OUT_SHARED}")

    # Display
    print(f"\n{'=' * 100}")
    print("CORY BLUMENFELD - OUTGOING COMMENTS (profiles he comments on)")
    print(f"{'=' * 100}")
    cory_out = [r for r in footprint_rows if r["operator"] == "cory" and r["direction"] == "outgoing"]
    print(f"{'#':<4} {'Slug':<40} {'Name':<30} {'Count':<6} {'Role':<12} {'Ring Also':<10}")
    print("-" * 102)
    for i, r in enumerate(cory_out[:30], 1):
        name = r["name"][:28] if r["name"] else ""
        print(f"{i:<4} {r['slug']:<40} {name:<30} {r['comment_count']:<6} {r['role']:<12} {r['ring_members_also_engaging']:<10}")
    print(f"Total profiles Cory comments on: {len(cory_out)}")

    print(f"\n{'=' * 100}")
    print("CHARLIE HILLS - OUTGOING COMMENTS (profiles he comments on)")
    print(f"{'=' * 100}")
    charlie_out = [r for r in footprint_rows if r["operator"] == "charlie" and r["direction"] == "outgoing"]
    print(f"{'#':<4} {'Slug':<40} {'Name':<30} {'Count':<6} {'Role':<12} {'Ring Also':<10}")
    print("-" * 102)
    for i, r in enumerate(charlie_out[:30], 1):
        name = r["name"][:28] if r["name"] else ""
        print(f"{i:<4} {r['slug']:<40} {name:<30} {r['comment_count']:<6} {r['role']:<12} {r['ring_members_also_engaging']:<10}")
    print(f"Total profiles Charlie comments on: {len(charlie_out)}")

    print(f"\n{'=' * 110}")
    print("SHARED TARGETS - Profiles BOTH Cory and Charlie comment on (confirmed client roster)")
    print(f"{'=' * 110}")
    print(f"{'#':<4} {'Slug':<38} {'Name':<28} {'Cory':<6} {'Charlie':<8} {'Combined':<9} {'GP':<6} {'Reg':<5} {'Role':<12}")
    print("-" * 116)
    for i, r in enumerate(shared_rows, 1):
        name = r["target_name"][:26] if r["target_name"] else ""
        print(f"{i:<4} {r['target_slug']:<38} {name:<28} {r['cory_comments']:<6} {r['charlie_comments']:<8} "
              f"{r['combined']:<9} {r['game_plan_comments']:<6} {'Y' if r['in_registry'] else '-':<5} {r['role']:<12}")

    print(f"\nTotal shared targets: {len(shared_rows)}")
    with_gp = sum(1 for r in shared_rows if r["game_plan_comments"] > 0)
    print(f"Also targeted by The Game Plan: {with_gp}")


if __name__ == "__main__":
    main()
