#!/usr/bin/env python3
"""
scrape_expansion.py — Scrape remaining expansion targets via Apify.

Reads data/expansion-targets.csv, checks which slugs already have scraped data,
and scrapes the rest in batches. Merges results into existing comments.csv and posts.csv.
"""

import csv
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

APIFY_BASE = "https://api.apify.com/v2"
ACTOR_ID = "harvestapi~linkedin-profile-posts"
POLL_INTERVAL = 15
POLL_TIMEOUT = 1800

# Import consolidation functions from scrape_linkedin
sys.path.insert(0, str(ROOT / "scripts"))
from scrape_linkedin import consolidate_posts, consolidate_comments, save_csv, slug_from_url


def get_scraped_slugs():
    """Check all existing raw JSON files to find which author slugs are already scraped."""
    scraped_dir = ROOT / "data" / "scraped"
    slugs = set()
    for f in sorted(scraped_dir.glob("batch_*_raw.json")):
        try:
            with open(f, encoding="utf-8", errors="replace") as fh:
                items = json.load(fh)
            for item in items:
                if item.get("type") == "post":
                    author = item.get("author") or {}
                    slug = author.get("publicIdentifier", "")
                    if slug:
                        slugs.add(slug.lower())
        except Exception:
            pass
    return slugs


def load_expansion_targets():
    """Load expansion-targets.csv and return list of target dicts."""
    path = ROOT / "data" / "expansion-targets.csv"
    targets = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            targets.append({
                "slug": row["slug"],
                "name": row["name"],
                "url": row["linkedin_url"],
            })
    return targets


def next_batch_num():
    """Find next available batch number."""
    scraped_dir = ROOT / "data" / "scraped"
    existing = [int(f.stem.split("_")[1]) for f in scraped_dir.glob("batch_*_raw.json")]
    return max(existing) + 1 if existing else 1


def run_batch(batch_num, urls, token, max_posts=30, max_comments=100):
    """Run one Apify actor batch. Returns (success, data_or_error)."""
    run_url = f"{APIFY_BASE}/acts/{ACTOR_ID}/runs?token={token}"
    payload = {
        "targetUrls": urls,
        "maxPosts": max_posts,
        "scrapeComments": True,
        "maxComments": max_comments,
        "scrapeReactions": False,
        "maxReactions": 0,
        "includeQuotePosts": True,
        "includeReposts": False,
    }

    print(f"  Starting Apify run for batch {batch_num} ({len(urls)} URLs)...")
    try:
        resp = requests.post(run_url, json=payload, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        return False, f"Failed to start run: {e}"

    run_data = resp.json().get("data", {})
    run_id = run_data.get("id")
    if not run_id:
        return False, f"No run ID in response: {resp.text[:200]}"

    print(f"  Run ID: {run_id} -- polling...")
    elapsed = 0
    while elapsed < POLL_TIMEOUT:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL
        try:
            status_resp = requests.get(
                f"{APIFY_BASE}/actor-runs/{run_id}?token={token}", timeout=15
            )
            status_resp.raise_for_status()
            status = status_resp.json().get("data", {}).get("status", "")
        except requests.RequestException as e:
            print(f"  Poll error: {e} -- retrying...")
            continue

        if status == "SUCCEEDED":
            print(f"  Batch {batch_num} succeeded ({elapsed}s)")
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return False, f"Run ended with status: {status}"
        else:
            mins = elapsed // 60
            secs = elapsed % 60
            print(f"  Status: {status} ({mins}m{secs}s elapsed)")
    else:
        return False, f"Timed out after {POLL_TIMEOUT}s"

    try:
        items_resp = requests.get(
            f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items?token={token}", timeout=60
        )
        items_resp.raise_for_status()
        return True, items_resp.json()
    except requests.RequestException as e:
        return False, f"Failed to fetch dataset: {e}"


def reconsolidate_all():
    """Re-read all raw JSON files and rebuild posts.csv and comments.csv."""
    scraped_dir = ROOT / "data" / "scraped"
    all_items = []
    for f in sorted(scraped_dir.glob("batch_*_raw.json")):
        try:
            with open(f, encoding="utf-8", errors="replace") as fh:
                items = json.load(fh)
            all_items.extend(items)
        except Exception as e:
            print(f"  Warning: could not read {f.name}: {e}")

    posts = consolidate_posts(all_items)
    comments = consolidate_comments(all_items)

    post_fields = [
        "post_id", "post_url", "author_name", "author_linkedin_url", "author_slug",
        "post_date", "content_preview", "likes", "comments_count", "shares",
        "has_image", "has_video", "has_document",
    ]
    comment_fields = [
        "post_id", "post_url", "post_author_slug", "comment_author_name",
        "comment_author_linkedin_url", "comment_author_slug", "comment_text",
        "comment_timestamp",
    ]

    save_csv(scraped_dir / "posts.csv", posts, post_fields)
    save_csv(scraped_dir / "comments.csv", comments, comment_fields)

    return posts, comments


def main():
    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("ERROR: APIFY_API_TOKEN not found in .env")
        sys.exit(1)

    print("Loading expansion targets...")
    all_targets = load_expansion_targets()
    print(f"  Total targets: {len(all_targets)}")

    print("Checking already-scraped slugs...")
    scraped_slugs = get_scraped_slugs()
    print(f"  Already scraped: {len(scraped_slugs)} unique slugs")

    remaining = [t for t in all_targets if t["slug"].lower() not in scraped_slugs]
    print(f"  Remaining to scrape: {len(remaining)}")

    if not remaining:
        print("All expansion targets already scraped.")
        return

    scraped_dir = ROOT / "data" / "scraped"
    scraped_dir.mkdir(parents=True, exist_ok=True)

    batch_start = next_batch_num()
    batch_size = 5
    succeeded = 0
    failed = 0
    total_scraped = 0

    for i in range(0, len(remaining), batch_size):
        batch = remaining[i:i + batch_size]
        batch_num = batch_start + (i // batch_size)
        urls = [t["url"] for t in batch]
        names = [t["name"] for t in batch]

        print(f"\n{'='*60}")
        print(f"BATCH {batch_num} ({len(batch)} profiles): {', '.join(names)}")
        print(f"{'='*60}")

        ok, result = run_batch(batch_num, urls, token)

        if ok:
            output_file = scraped_dir / f"batch_{batch_num}_raw.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            print(f"  Saved raw JSON to {output_file}")
            succeeded += 1
            total_scraped += len(batch)
            print(f"  Running total: {total_scraped}/{len(remaining)} profiles scraped")
        else:
            print(f"  ERROR: {result}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETE")
    print(f"  Batches succeeded: {succeeded}")
    print(f"  Batches failed: {failed}")
    print(f"  Profiles scraped: {total_scraped}/{len(remaining)}")
    print(f"{'='*60}")

    print("\nReconsolidating all data...")
    posts, comments = reconsolidate_all()
    print(f"\nFinal totals: {len(posts)} posts, {len(comments)} comments")


if __name__ == "__main__":
    main()
