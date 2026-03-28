#!/usr/bin/env python3
"""Task 5: Full VA identification using multiple behavioral signals."""

import csv
import os
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMENTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "comments.csv")
POSTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "posts.csv")
PROFILES_CSV = os.path.join(BASE_DIR, "registry", "profiles.csv")
CENSUS_CSV = os.path.join(BASE_DIR, "data", "analysis", "full-commenter-census.csv")
QUALITY_CSV = os.path.join(BASE_DIR, "data", "analysis", "comment-quality.csv")
TIMING_CSV = os.path.join(BASE_DIR, "data", "analysis", "comment-timing.csv")
BURST_CSV = os.path.join(BASE_DIR, "data", "analysis", "burst-events.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "data", "analysis", "va-identification.csv")

KEY_SLUGS = {
    "cory": "coryblumenfeld",
    "charlie": "charlie-hills",
    "shane": "shanebarker",
}


def load_registry():
    reg = {}
    with open(PROFILES_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug = row["linkedin_url"].rstrip("/").split("/")[-1]
            reg[slug] = row
    return reg


def load_census():
    census = {}
    with open(CENSUS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            census[row["commenter_slug"]] = row
    return census


def load_quality():
    quality = {}
    with open(QUALITY_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            quality[row["commenter_slug"]] = row
    return quality


def load_timing():
    timing = {}
    with open(TIMING_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            timing[row["commenter_slug"]] = row
    return timing


def load_burst_counts():
    counts = defaultdict(int)
    with open(BURST_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slugs = row["commenter_slugs"].split(";")
            for slug in slugs:
                counts[slug.strip()] += 1
    return counts


def main():
    print("Loading data...")
    census = load_census()
    quality = load_quality()
    timing = load_timing()
    burst_counts = load_burst_counts()
    registry = load_registry()

    # Build comment indexes from raw data
    comments = []
    with open(COMMENTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            comments.append(row)

    # Build commenter -> {author -> count} for key profile checks
    commenter_to_authors = defaultdict(lambda: defaultdict(int))
    commenter_info = {}
    for c in comments:
        slug = c["comment_author_slug"]
        commenter_to_authors[slug][c["post_author_slug"]] += 1
        if slug not in commenter_info:
            commenter_info[slug] = {
                "name": c["comment_author_name"],
                "url": c["comment_author_linkedin_url"],
            }

    # Calculate unique text ratio per commenter
    commenter_texts = defaultdict(list)
    for c in comments:
        text = c["comment_text"].strip()
        if text:
            commenter_texts[c["comment_author_slug"]].append(text)

    unique_text_ratios = {}
    for slug, texts in commenter_texts.items():
        if len(texts) > 0:
            unique_texts = len(set(texts))
            unique_text_ratios[slug] = unique_texts / len(texts)

    print(f"  {len(census)} census entries, {len(quality)} quality entries, "
          f"{len(timing)} timing entries, {len(burst_counts)} burst participants")

    # Filter: accounts commenting on 15+ unique authors
    candidates = {}
    for slug, cdata in census.items():
        unique_authors = int(cdata["unique_authors_commented_on"])
        if unique_authors >= 15:
            candidates[slug] = {
                "slug": slug,
                "name": cdata["commenter_name"],
                "linkedin_url": cdata["linkedin_url"],
                "total_comments": int(cdata["total_comments"]),
                "unique_authors": unique_authors,
            }

    print(f"\nCandidates with 15+ unique authors: {len(candidates)}")

    # Score each candidate
    results = []
    for slug, cand in candidates.items():
        signals = []
        va_score = 0

        # Get quality data
        q = quality.get(slug, {})
        pct_generic = float(q.get("pct_generic", 0))
        utr = unique_text_ratios.get(slug, 1.0)

        # Get timing data
        t = timing.get(slug, {})
        avg_response = float(t.get("avg_response_minutes", 9999))

        # Get burst count
        bc = burst_counts.get(slug, 0)

        # Check key profile comments
        comments_on_cory = commenter_to_authors[slug].get(KEY_SLUGS["cory"], 0)
        comments_on_charlie = commenter_to_authors[slug].get(KEY_SLUGS["charlie"], 0)
        comments_on_shane = commenter_to_authors[slug].get(KEY_SLUGS["shane"], 0)

        # Signal 1: pct_generic > 40%
        if pct_generic > 40:
            va_score += 1
            signals.append(f"pct_generic={pct_generic:.1f}%")

        # Signal 2: unique_text_ratio < 0.4
        if utr < 0.4:
            va_score += 1
            signals.append(f"unique_text_ratio={utr:.2f}")

        # Signal 3: avg_response < 120 minutes
        if avg_response < 120:
            va_score += 1
            signals.append(f"avg_response={avg_response:.0f}min")

        # Signal 4: 3+ burst events
        if bc >= 3:
            va_score += 1
            signals.append(f"burst_count={bc}")

        # Signal 5: Comments on all three operators/clients (Cory AND Charlie AND Shane)
        if comments_on_cory > 0 and comments_on_charlie > 0 and comments_on_shane > 0:
            va_score += 1
            signals.append(f"all_operators(cory={comments_on_cory},charlie={comments_on_charlie},shane={comments_on_shane})")

        # Must have at least 1 signal to qualify
        if va_score >= 1:
            info = commenter_info.get(slug, {})
            results.append({
                "slug": slug,
                "name": cand["name"] or info.get("name", ""),
                "linkedin_url": cand["linkedin_url"] or info.get("url", ""),
                "va_score": va_score,
                "total_comments": cand["total_comments"],
                "unique_authors": cand["unique_authors"],
                "pct_generic": round(pct_generic, 1),
                "unique_text_ratio": round(utr, 2),
                "avg_response_min": round(avg_response, 1) if avg_response < 9999 else "",
                "burst_count": bc,
                "comments_on_cory": comments_on_cory,
                "comments_on_charlie": comments_on_charlie,
                "comments_on_shane": comments_on_shane,
                "signal_flags": ",".join(signals),
            })

    results.sort(key=lambda x: (-x["va_score"], -x["unique_authors"]))

    # Write output
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "slug", "name", "linkedin_url", "va_score", "total_comments",
            "unique_authors", "pct_generic", "unique_text_ratio", "avg_response_min",
            "burst_count", "comments_on_cory", "comments_on_charlie", "comments_on_shane",
            "signal_flags"
        ])
        writer.writeheader()
        writer.writerows(results)

    print(f"Wrote {len(results)} VA candidates to {OUTPUT_CSV}")

    # Display va_score >= 3
    strong = [r for r in results if r["va_score"] >= 3]
    print(f"\n{'=' * 120}")
    print(f"STRONG VA INDICATORS (score >= 3): {len(strong)} accounts")
    print(f"{'=' * 120}")
    print(f"{'#':<4} {'Slug':<38} {'Name':<28} {'Score':<6} {'Cmts':<6} {'Auth':<5} "
          f"{'Generic':<8} {'UTR':<6} {'AvgMin':<7} {'Burst':<6} {'Signals'}")
    print("-" * 120)
    for i, r in enumerate(strong, 1):
        name = r["name"][:26] if r["name"] else ""
        avg = str(r["avg_response_min"]) if r["avg_response_min"] else "-"
        print(f"{i:<4} {r['slug']:<38} {name:<28} {r['va_score']:<6} {r['total_comments']:<6} "
              f"{r['unique_authors']:<5} {r['pct_generic']:<8} {r['unique_text_ratio']:<6} "
              f"{avg:<7} {r['burst_count']:<6} {r['signal_flags']}")

    # Display va_score >= 2 sorted by unique_authors
    moderate = [r for r in results if r["va_score"] >= 2]
    moderate.sort(key=lambda x: -x["unique_authors"])
    print(f"\n{'=' * 120}")
    print(f"MODERATE+ VA INDICATORS (score >= 2): {len(moderate)} accounts")
    print(f"{'=' * 120}")
    print(f"{'#':<4} {'Slug':<38} {'Name':<28} {'Score':<6} {'Cmts':<6} {'Auth':<5} "
          f"{'Generic':<8} {'UTR':<6} {'AvgMin':<7} {'Burst':<6} {'Signals'}")
    print("-" * 120)
    for i, r in enumerate(moderate, 1):
        name = r["name"][:26] if r["name"] else ""
        avg = str(r["avg_response_min"]) if r["avg_response_min"] else "-"
        print(f"{i:<4} {r['slug']:<38} {name:<28} {r['va_score']:<6} {r['total_comments']:<6} "
              f"{r['unique_authors']:<5} {r['pct_generic']:<8} {r['unique_text_ratio']:<6} "
              f"{avg:<7} {r['burst_count']:<6} {r['signal_flags']}")

    # Summary
    print(f"\n{'=' * 80}")
    print("VA IDENTIFICATION SUMMARY")
    print(f"{'=' * 80}")
    for score in range(5, 0, -1):
        count = len([r for r in results if r["va_score"] == score])
        if count > 0:
            print(f"  Score {score}: {count} accounts")
    print(f"  Total flagged (score >= 1): {len(results)}")
    print(f"  Strong VA (score >= 3): {len(strong)}")
    print(f"  Moderate+ (score >= 2): {len(moderate)}")


if __name__ == "__main__":
    main()
