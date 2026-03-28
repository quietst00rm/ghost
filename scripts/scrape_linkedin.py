#!/usr/bin/env python3
"""
scrape_linkedin.py — Pull posts and comments from LinkedIn profiles via Apify.

Usage:
    python scripts/scrape_linkedin.py --mode profiles --dry-run
    python scripts/scrape_linkedin.py --mode profiles --batch-size 5
    python scripts/scrape_linkedin.py --mode single-url --url https://linkedin.com/in/someone
    python scripts/scrape_linkedin.py --mode profiles --resume
"""

import argparse
import csv
import json
import os
import re
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
POLL_TIMEOUT = 1800  # 30 minutes


def load_registry_targets():
    """Read profiles.csv, return list of unique LinkedIn URLs for confirmed, non-excluded profiles."""
    profiles_path = ROOT / "registry" / "profiles.csv"
    targets = []
    seen_urls = set()
    with open(profiles_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("linkedin_url", "").strip()
            confidence = row.get("confidence", "").strip().lower()
            role = row.get("role", "").strip().lower()
            if not url:
                continue
            if confidence != "confirmed":
                continue
            if role == "non-participant":
                continue
            normalized = url.rstrip("/").lower()
            if normalized not in seen_urls:
                seen_urls.add(normalized)
                targets.append({
                    "url": url,
                    "name": row.get("display_name", ""),
                    "role": role,
                    "profile_id": row.get("profile_id", ""),
                })
    return targets


def slug_from_url(url):
    """Extract the LinkedIn slug from a profile or post URL."""
    parsed = urlparse(url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) >= 2 and parts[0] == "in":
        return parts[1]
    return parts[-1] if parts else ""


def batch_list(items, size):
    """Split a list into batches of given size."""
    for i in range(0, len(items), size):
        yield i // size + 1, items[i : i + size]


def run_batch(batch_num, urls, token, max_posts, max_comments):
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

    print(f"  Run ID: {run_id} — polling...")
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
            print(f"  Poll error: {e} — retrying...")
            continue

        if status == "SUCCEEDED":
            print(f"  Batch {batch_num} succeeded ({elapsed}s)")
            break
        elif status in ("FAILED", "ABORTED", "TIMED-OUT"):
            return False, f"Run ended with status: {status}"
        else:
            print(f"  Status: {status} ({elapsed}s elapsed)", end="\r")
    else:
        return False, f"Timed out after {POLL_TIMEOUT}s"

    # Fetch dataset items
    try:
        items_resp = requests.get(
            f"{APIFY_BASE}/actor-runs/{run_id}/dataset/items?token={token}", timeout=60
        )
        items_resp.raise_for_status()
        return True, items_resp.json()
    except requests.RequestException as e:
        return False, f"Failed to fetch dataset: {e}"


def consolidate_posts(all_items):
    """Extract posts from raw Apify items into flat dicts.

    Apify harvestapi~linkedin-profile-posts returns flat items with type="post".
    Post schema: id, linkedinUrl, content, author (dict with name, linkedinUrl,
    publicIdentifier), postedAt (dict with date), engagement (dict with likes,
    comments, shares), postImages, comments (list of embedded comments).
    """
    posts = []
    seen_ids = set()
    for item in all_items:
        if item.get("type") != "post":
            continue
        post_id = str(item.get("entityId", "") or item.get("id", ""))
        if not post_id or post_id in seen_ids:
            continue
        seen_ids.add(post_id)

        post_url = item.get("linkedinUrl", "")
        author = item.get("author") or {}
        author_url = author.get("linkedinUrl", "")
        author_name = author.get("name", "")
        author_slug = author.get("publicIdentifier", "") or (slug_from_url(author_url) if author_url else "")
        content = item.get("content", "") or ""
        posted_at = item.get("postedAt") or {}
        post_date = posted_at.get("date", "")
        engagement = item.get("engagement") or {}

        posts.append({
            "post_id": post_id,
            "post_url": post_url,
            "author_name": author_name,
            "author_linkedin_url": author_url,
            "author_slug": author_slug,
            "post_date": post_date,
            "content_preview": content[:200].replace("\n", " ").replace("\r", " "),
            "likes": engagement.get("likes", 0) or 0,
            "comments_count": engagement.get("comments", 0) or 0,
            "shares": engagement.get("shares", 0) or 0,
            "has_image": bool(item.get("postImages")),
            "has_video": bool(item.get("video")),
            "has_document": bool(item.get("document")),
        })
    return posts


def consolidate_comments(all_items):
    """Extract comments from raw Apify items into flat dicts.

    Apify returns two kinds of comment data:
    1. Flat items with type="comment": actor (dict), commentary, postId, query.post
    2. Embedded comments inside type="post" items: comments[] list

    We use flat comment items (type="comment") as primary source, and also
    extract from embedded post comments for completeness. Deduplicate by comment id.
    """
    comments = []
    seen_comment_ids = set()

    # Build a lookup: postId -> post author slug (from post items)
    post_authors = {}
    for item in all_items:
        if item.get("type") == "post":
            pid = str(item.get("entityId", "") or item.get("id", ""))
            author = item.get("author") or {}
            post_authors[pid] = author.get("publicIdentifier", "") or slug_from_url(author.get("linkedinUrl", ""))

    # 1. Flat comment items (type="comment")
    for item in all_items:
        if item.get("type") != "comment":
            continue
        comment_id = str(item.get("id", ""))
        if comment_id in seen_comment_ids:
            continue
        seen_comment_ids.add(comment_id)

        actor = item.get("actor") or {}
        post_id = str(item.get("postId", ""))
        query = item.get("query") or {}
        post_url = query.get("post", "")
        post_author_slug = post_authors.get(post_id, "")
        # Try to extract post author slug from the post URL if not in lookup
        if not post_author_slug and post_url:
            url_match = re.search(r"linkedin\.com/posts/([^_/]+)", post_url)
            if url_match:
                post_author_slug = url_match.group(1)

        commenter_url = actor.get("linkedinUrl", "")
        commenter_name = actor.get("name", "")
        commenter_slug = actor.get("publicIdentifier", "") or (slug_from_url(commenter_url) if commenter_url else "")
        created_at = item.get("createdAt", "")

        comments.append({
            "post_id": post_id,
            "post_url": post_url,
            "post_author_slug": post_author_slug,
            "comment_author_name": commenter_name,
            "comment_author_linkedin_url": commenter_url,
            "comment_author_slug": commenter_slug,
            "comment_text": (item.get("commentary", "") or "").replace("\n", " ").replace("\r", " "),
            "comment_timestamp": created_at,
        })

    # 2. Embedded comments inside post items
    for item in all_items:
        if item.get("type") != "post":
            continue
        post_id = str(item.get("entityId", "") or item.get("id", ""))
        post_url = item.get("linkedinUrl", "")
        author = item.get("author") or {}
        post_author_slug = author.get("publicIdentifier", "") or slug_from_url(author.get("linkedinUrl", ""))

        for c in item.get("comments") or []:
            cid = str(c.get("id", ""))
            if cid in seen_comment_ids:
                continue
            seen_comment_ids.add(cid)

            c_actor = c.get("author") or c.get("actor") or {}
            c_url = c_actor.get("linkedinUrl", "")
            c_name = c_actor.get("name", "")
            c_slug = c_actor.get("publicIdentifier", "") or (slug_from_url(c_url) if c_url else "")

            comments.append({
                "post_id": post_id,
                "post_url": post_url,
                "post_author_slug": post_author_slug,
                "comment_author_name": c_name,
                "comment_author_linkedin_url": c_url,
                "comment_author_slug": c_slug,
                "comment_text": (c.get("commentary", "") or c.get("text", "") or "").replace("\n", " ").replace("\r", " "),
                "comment_timestamp": c.get("createdAt", "") or c.get("postedDate", ""),
            })

    return comments


def save_csv(filepath, rows, fieldnames):
    """Write list of dicts to CSV."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Saved {len(rows)} rows to {filepath}")


def print_summary(targets, succeeded, failed, posts, comments):
    """Print scrape summary."""
    print("\n" + "=" * 60)
    print("SCRAPE SUMMARY")
    print("=" * 60)
    print(f"  Profiles attempted:  {len(targets)}")
    print(f"  Batches succeeded:   {succeeded}")
    print(f"  Batches failed:      {failed}")
    print(f"  Total posts:         {len(posts)}")
    print(f"  Total comments:      {len(comments)}")

    if posts:
        dates = [p["post_date"] for p in posts if p["post_date"]]
        if dates:
            print(f"  Post date range:     {min(dates)} to {max(dates)}")

        print(f"\n  Top 10 most-commented posts:")
        top = sorted(posts, key=lambda p: int(p["comments_count"]), reverse=True)[:10]
        for i, p in enumerate(top, 1):
            print(f"    {i:2}. [{p['comments_count']:>4} comments] {p['author_slug']}: {p['content_preview'][:60]}...")

    print("=" * 60)


def print_dry_run(targets, batch_size, max_posts, max_comments):
    """Print dry-run plan without making API calls."""
    print("=" * 60)
    print("DRY RUN — No API calls will be made")
    print("=" * 60)
    print(f"\n  Mode:            profiles (registry)")
    print(f"  Batch size:      {batch_size}")
    print(f"  Max posts:       {max_posts}")
    print(f"  Max comments:    {max_comments}")
    print(f"  Total targets:   {len(targets)}")
    total_batches = (len(targets) + batch_size - 1) // batch_size
    print(f"  Total batches:   {total_batches}")

    print(f"\n  TARGET PROFILES ({len(targets)}):")
    print(f"  {'#':<4} {'ID':<6} {'Slug':<35} {'Role':<12} {'Name'}")
    print(f"  {'—'*4} {'—'*6} {'—'*35} {'—'*12} {'—'*20}")
    for i, t in enumerate(targets, 1):
        slug = slug_from_url(t["url"])
        print(f"  {i:<4} {t['profile_id']:<6} {slug:<35} {t['role']:<12} {t['name']}")

    print(f"\n  BATCH PLAN:")
    for batch_num, batch in batch_list(targets, batch_size):
        slugs = [slug_from_url(t["url"]) for t in batch]
        output_file = ROOT / "data" / "scraped" / f"batch_{batch_num}_raw.json"
        exists = output_file.exists()
        status = " [EXISTS — will skip with --resume]" if exists else ""
        print(f"\n  Batch {batch_num} ({len(batch)} URLs){status}:")
        for s in slugs:
            print(f"    - {s}")

    # Estimate
    est_posts = len(targets) * max_posts
    est_comments = est_posts * max_comments
    print(f"\n  ESTIMATES (upper bound):")
    print(f"    Max posts:     {est_posts:,}")
    print(f"    Max comments:  {est_comments:,}")
    print(f"    Apify runs:    {total_batches}")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Scrape LinkedIn profiles via Apify")
    parser.add_argument("--mode", choices=["profiles", "single-url"], required=True)
    parser.add_argument("--url", help="LinkedIn URL (required for single-url mode)")
    parser.add_argument("--dry-run", action="store_true", help="Print plan, no API calls")
    parser.add_argument("--batch-size", type=int, default=5)
    parser.add_argument("--max-posts", type=int, default=30)
    parser.add_argument("--max-comments", type=int, default=100)
    parser.add_argument("--resume", action="store_true", help="Skip batches with existing output")
    args = parser.parse_args()

    if args.mode == "single-url":
        if not args.url:
            print("ERROR: --url is required when mode is single-url")
            sys.exit(1)
        targets = [{"url": args.url, "name": "", "role": "", "profile_id": "—"}]
    else:
        targets = load_registry_targets()
        if not targets:
            print("ERROR: No confirmed profiles with LinkedIn URLs found in registry.")
            sys.exit(1)

    if args.dry_run:
        print_dry_run(targets, args.batch_size, args.max_posts, args.max_comments)
        return

    token = os.getenv("APIFY_API_TOKEN")
    if not token:
        print("ERROR: APIFY_API_TOKEN not found in .env")
        sys.exit(1)

    scraped_dir = ROOT / "data" / "scraped"
    scraped_dir.mkdir(parents=True, exist_ok=True)

    all_items = []
    succeeded = 0
    failed = 0

    for batch_num, batch in batch_list(targets, args.batch_size):
        output_file = scraped_dir / f"batch_{batch_num}_raw.json"

        if args.resume and output_file.exists():
            print(f"  Batch {batch_num}: skipping (output exists, --resume)")
            with open(output_file, "r", encoding="utf-8") as f:
                all_items.extend(json.load(f))
            succeeded += 1
            continue

        urls = [t["url"] for t in batch]
        ok, result = run_batch(batch_num, urls, token, args.max_posts, args.max_comments)

        if ok:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2)
            print(f"  Saved raw JSON to {output_file}")
            all_items.extend(result)
            succeeded += 1
        else:
            print(f"  ERROR batch {batch_num}: {result}")
            failed += 1

    # Consolidate
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

    print_summary(targets, succeeded, failed, posts, comments)


if __name__ == "__main__":
    main()
