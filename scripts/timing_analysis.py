#!/usr/bin/env python3
"""
Comment Timing Analysis: Identifies burst patterns and response time anomalies.
Finds coordinated commenting by measuring time-to-comment and burst events.
"""

import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPED_DIR = os.path.join(BASE, "data", "scraped")
CENSUS_FILE = os.path.join(BASE, "data", "analysis", "full-commenter-census.csv")
OUTPUT_TIMING = os.path.join(BASE, "data", "analysis", "comment-timing.csv")
OUTPUT_BURSTS = os.path.join(BASE, "data", "analysis", "burst-events.csv")


def parse_ts(ts_str):
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return None
    try:
        # Handle various ISO formats
        ts_str = ts_str.replace("Z", "+00:00")
        if "." in ts_str:
            ts_str = ts_str.split(".")[0] + ts_str.split(".")[-1][-6:] if "+" in ts_str.split(".")[-1] else ts_str.split(".")[0]
        return datetime.fromisoformat(ts_str.replace("+00:00", ""))
    except (ValueError, IndexError):
        try:
            return datetime.strptime(ts_str[:19], "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            return None


def load_high_freq_slugs():
    """Load slugs of commenters with 10+ unique authors from census."""
    slugs = set()
    with open(CENSUS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if int(row["unique_authors_commented_on"]) >= 10:
                slugs.add(row["commenter_slug"])
    return slugs


def load_posts():
    """Load posts with their publication timestamps."""
    posts = {}
    path = os.path.join(SCRAPED_DIR, "posts.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            post_id = row["post_id"]
            posts[post_id] = {
                "url": row["post_url"],
                "author": row["author_slug"],
                "date": parse_ts(row["post_date"]),
            }
    return posts


def load_comments():
    """Load all comments with timestamps."""
    comments = []
    path = os.path.join(SCRAPED_DIR, "comments.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            comments.append(row)
    return comments


def analyze_timing(comments, posts, high_freq):
    """Calculate response times for high-frequency commenters."""
    # commenter_slug -> list of response times in minutes
    response_times = defaultdict(list)
    commenter_names = {}

    for c in comments:
        slug = c["comment_author_slug"]
        if not slug:
            continue

        commenter_names[slug] = c["comment_author_name"]
        post_id = c["post_id"]
        comment_ts = parse_ts(c.get("comment_timestamp", ""))

        if not comment_ts or post_id not in posts:
            continue

        post_ts = posts[post_id]["date"]
        if not post_ts:
            continue

        delta = (comment_ts - post_ts).total_seconds() / 60.0
        # Only count positive deltas (comment after post) and within 30 days
        if 0 < delta < 43200:  # 30 days in minutes
            response_times[slug].append(delta)

    # Build timing stats for high-freq commenters
    timing_rows = []
    for slug in high_freq:
        times = response_times.get(slug, [])
        if not times:
            continue

        total = len(times)
        avg_min = sum(times) / total
        within_60 = sum(1 for t in times if t <= 60) / total * 100
        within_120 = sum(1 for t in times if t <= 120) / total * 100

        timing_rows.append({
            "commenter_slug": slug,
            "name": commenter_names.get(slug, ""),
            "total_comments_with_timing": total,
            "avg_response_minutes": round(avg_min, 1),
            "pct_within_60min": round(within_60, 1),
            "pct_within_120min": round(within_120, 1),
        })

    return timing_rows, response_times, commenter_names


def find_bursts(comments, posts, high_freq, window_minutes=60):
    """Find burst events: 3+ high-freq commenters within a time window on same post."""
    # Group comments by post
    post_comments = defaultdict(list)
    for c in comments:
        slug = c["comment_author_slug"]
        if not slug or slug not in high_freq:
            continue
        ts = parse_ts(c.get("comment_timestamp", ""))
        if ts:
            post_comments[c["post_id"]].append({
                "slug": slug,
                "name": c["comment_author_name"],
                "ts": ts,
            })

    bursts = []
    for post_id, clist in post_comments.items():
        if len(clist) < 3:
            continue

        # Sort by timestamp
        clist.sort(key=lambda x: x["ts"])

        # Sliding window
        for i in range(len(clist)):
            window_end = clist[i]["ts"] + timedelta(minutes=window_minutes)
            burst_commenters = set()
            burst_names = []
            earliest = clist[i]["ts"]
            latest = clist[i]["ts"]

            for j in range(i, len(clist)):
                if clist[j]["ts"] <= window_end:
                    if clist[j]["slug"] not in burst_commenters:
                        burst_commenters.add(clist[j]["slug"])
                        burst_names.append(clist[j]["slug"])
                        latest = clist[j]["ts"]
                else:
                    break

            if len(burst_commenters) >= 3:
                window_actual = (latest - earliest).total_seconds() / 60.0
                post_info = posts.get(post_id, {})
                bursts.append({
                    "post_url": post_info.get("url", ""),
                    "post_author": post_info.get("author", ""),
                    "post_id": post_id,
                    "burst_timestamp": earliest.isoformat(),
                    "commenters_in_burst": len(burst_commenters),
                    "burst_window_minutes": round(window_actual, 1),
                    "commenter_slugs": ";".join(sorted(burst_commenters)),
                })

    # Deduplicate: keep the largest burst per post
    best_bursts = {}
    for b in bursts:
        key = b["post_id"]
        if key not in best_bursts or b["commenters_in_burst"] > best_bursts[key]["commenters_in_burst"]:
            best_bursts[key] = b

    return sorted(best_bursts.values(), key=lambda x: x["commenters_in_burst"], reverse=True)


def write_outputs(timing_rows, bursts):
    """Write timing and burst CSVs."""
    os.makedirs(os.path.dirname(OUTPUT_TIMING), exist_ok=True)

    # Timing CSV
    timing_rows.sort(key=lambda r: r["avg_response_minutes"])
    if timing_rows:
        with open(OUTPUT_TIMING, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=timing_rows[0].keys())
            writer.writeheader()
            writer.writerows(timing_rows)

    # Bursts CSV
    if bursts:
        with open(OUTPUT_BURSTS, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=bursts[0].keys())
            writer.writeheader()
            writer.writerows(bursts)


def display_results(timing_rows, bursts):
    """Print summary."""
    print("=" * 90)
    print("COMMENT TIMING ANALYSIS")
    print("=" * 90)

    print(f"\nHigh-frequency commenters with timing data: {len(timing_rows)}")

    print(f"\n{'='*90}")
    print("TOP 30 FASTEST RESPONDERS (lowest avg response time, 10+ timed comments)")
    print(f"{'='*90}")
    print(f"{'Slug':<40} {'Avg Min':>8} {'<60m%':>7} {'<120m%':>7} {'Comments':>8}")
    print("-" * 90)
    fast = [r for r in timing_rows if r["total_comments_with_timing"] >= 10]
    fast.sort(key=lambda r: r["avg_response_minutes"])
    for r in fast[:30]:
        print(f"{r['commenter_slug']:<40} {r['avg_response_minutes']:>8} {r['pct_within_60min']:>6.1f}% {r['pct_within_120min']:>6.1f}% {r['total_comments_with_timing']:>8}")

    print(f"\n{'='*90}")
    print(f"TOP 20 BURST EVENTS (most coordinated commenters in 60-min window)")
    print(f"{'='*90}")
    print(f"{'Author':<30} {'#Burst':>6} {'Window':>8} {'Post URL (truncated)'}")
    print("-" * 90)
    for b in bursts[:20]:
        url_short = b["post_url"][:50] + "..." if len(b["post_url"]) > 50 else b["post_url"]
        print(f"{b['post_author']:<30} {b['commenters_in_burst']:>6} {b['burst_window_minutes']:>7.1f}m {url_short}")
        # Print the commenters in this burst
        slugs = b["commenter_slugs"].split(";")
        print(f"  Commenters: {', '.join(slugs[:10])}")
        if len(slugs) > 10:
            print(f"  ... and {len(slugs) - 10} more")

    print(f"\nTotal burst events (3+ high-freq commenters in 60min): {len(bursts)}")


if __name__ == "__main__":
    print("Loading census for high-frequency slugs...")
    high_freq = load_high_freq_slugs()
    print(f"High-frequency commenters (10+ authors): {len(high_freq)}")

    print("Loading posts...")
    posts = load_posts()
    print(f"Posts: {len(posts):,}")

    print("Loading comments...")
    comments = load_comments()
    print(f"Comments: {len(comments):,}")

    print("Analyzing timing...")
    timing_rows, response_times, names = analyze_timing(comments, posts, high_freq)

    print("Finding burst patterns...")
    bursts = find_bursts(comments, posts, high_freq)

    print("Writing outputs...")
    write_outputs(timing_rows, bursts)

    display_results(timing_rows, bursts)
    print(f"\nOutputs:")
    print(f"  {OUTPUT_TIMING}")
    print(f"  {OUTPUT_BURSTS}")
