#!/usr/bin/env python3
"""
build_reciprocal_pairs.py — Identify reciprocal engagement pairs from top-pairs.csv.

A reciprocal pair exists when A comments on B's posts AND B comments on A's posts,
both with count >= 3. This is the core signal of coordinated engagement.

Outputs:
    data/analysis/reciprocal-pairs.csv — All reciprocal pairs with counts
"""

import csv
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
                "network": row.get("network", ""),
            }
    return profiles


def load_top_pairs():
    """Load top-pairs.csv into a dict of (commenter, author) -> count."""
    pairs = {}
    path = ROOT / "data" / "analysis" / "top-pairs.csv"
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row["commenter_slug"], row["author_slug"])
            pairs[key] = int(row["comment_count"])
    return pairs


def find_reciprocal_pairs(pair_counts, min_count=3):
    """Find all pairs where A->B >= min_count AND B->A >= min_count."""
    reciprocal = []
    seen = set()

    for (a, b), a_to_b in pair_counts.items():
        if a_to_b < min_count:
            continue
        b_to_a = pair_counts.get((b, a), 0)
        if b_to_a < min_count:
            continue

        key = tuple(sorted([a, b]))
        if key in seen:
            continue
        seen.add(key)

        reciprocal.append({
            "profile_a": key[0],
            "profile_b": key[1],
            "a_to_b_count": pair_counts.get((key[0], key[1]), 0),
            "b_to_a_count": pair_counts.get((key[1], key[0]), 0),
        })

    # Sort by total descending
    for r in reciprocal:
        r["total"] = r["a_to_b_count"] + r["b_to_a_count"]
    reciprocal.sort(key=lambda r: r["total"], reverse=True)
    return reciprocal


def main():
    print("Loading registry...")
    registry = load_registry()
    print(f"  {len(registry)} profiles with LinkedIn URLs")

    print("Loading top pairs...")
    pair_counts = load_top_pairs()
    print(f"  {len(pair_counts)} directed pairs")

    print("\nFinding reciprocal pairs (both directions >= 3)...")
    reciprocal = find_reciprocal_pairs(pair_counts, min_count=3)

    # Add role info
    for r in reciprocal:
        a_info = registry.get(r["profile_a"], {})
        b_info = registry.get(r["profile_b"], {})
        r["a_role"] = a_info.get("role", "unknown")
        r["b_role"] = b_info.get("role", "unknown")

    output_path = ROOT / "data" / "analysis" / "reciprocal-pairs.csv"
    fields = ["profile_a", "profile_b", "a_to_b_count", "b_to_a_count", "total", "a_role", "b_role"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(reciprocal)

    print(f"  Found {len(reciprocal)} reciprocal pairs")
    print(f"  Saved to {output_path}")

    # Breakdown by role pairing
    role_pairs = {}
    for r in reciprocal:
        key = tuple(sorted([r["a_role"], r["b_role"]]))
        role_pairs[key] = role_pairs.get(key, 0) + 1

    print("\n  RECIPROCAL PAIRS BY ROLE COMBINATION:")
    for roles, count in sorted(role_pairs.items(), key=lambda x: -x[1]):
        print(f"    {roles[0]} <-> {roles[1]}: {count}")

    # Threshold breakdown
    print("\n  PAIRS BY THRESHOLD:")
    for threshold in [3, 5, 10, 20, 30]:
        count = len([r for r in reciprocal if r["total"] >= threshold * 2])
        print(f"    Total >= {threshold*2} (avg {threshold}+ each way): {count}")

    print(f"\n  TOP 30 RECIPROCAL PAIRS:")
    print(f"  {'#':<4} {'Total':<7} {'A->B':<6} {'B->A':<6} {'Profile A':<30} {'Profile B':<30}")
    print(f"  {'='*4} {'='*7} {'='*6} {'='*6} {'='*30} {'='*30}")
    for i, r in enumerate(reciprocal[:30], 1):
        a_name = registry.get(r["profile_a"], {}).get("name", r["profile_a"])
        b_name = registry.get(r["profile_b"], {}).get("name", r["profile_b"])
        print(f"  {i:<4} {r['total']:<7} {r['a_to_b_count']:<6} {r['b_to_a_count']:<6} {a_name:<30} {b_name:<30}")


if __name__ == "__main__":
    main()
