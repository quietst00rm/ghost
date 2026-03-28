#!/usr/bin/env python3
"""
Comment Text Quality Analysis: Identifies VA-like commenting behavior.
Flags accounts with high generic phrase usage and low text uniqueness.
"""

import csv
import os
import re
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRAPED_DIR = os.path.join(BASE, "data", "scraped")
CENSUS_FILE = os.path.join(BASE, "data", "analysis", "full-commenter-census.csv")
OUTPUT = os.path.join(BASE, "data", "analysis", "comment-quality.csv")

# Generic phrases commonly used in engagement pods
GENERIC_PHRASES = [
    r"\bgreat post\b",
    r"\blove this\b",
    r"\bso true\b",
    r"\bwell said\b",
    r"\bthanks for sharing\b",
    r"\bgreat insight\b",
    r"\bspot on\b",
    r"\bnailed it\b",
    r"\bthis is gold\b",
    r"\bgreat share\b",
    r"\bawesome post\b",
    r"\bgreat point\b",
    r"\bso important\b",
    r"\babsolutely\b",
    r"\b100%\b",
    r"\bpowerful\b",
    r"\binspiring\b",
    r"\bbrilliant\b",
    r"\bamazing\b",
    r"\bincredible\b",
    r"\bfantastic\b",
    r"\bpreach\b",
    r"\bthis right here\b",
    r"\bthis is it\b",
    r"\bkeep it up\b",
    r"\bkeep going\b",
    r"\bgreat content\b",
    r"\bvaluable\b",
    r"\bresonate\b",
    r"\bneeded this\b",
    r"\bneeded to hear this\b",
    r"\bgame changer\b",
    r"\bmind blown\b",
    r"\bmic drop\b",
    r"\bcouldn.t agree more\b",
    r"\bcould not agree more\b",
    r"\bagree with this\b",
    r"\btotally agree\b",
]

# Compile patterns
GENERIC_PATTERNS = [re.compile(p, re.IGNORECASE) for p in GENERIC_PHRASES]

# Emoji-only pattern
EMOJI_ONLY = re.compile(
    r"^[\s\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    r"\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251"
    r"\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF"
    r"\U00002600-\U000026FF\U00002700-\U000027BF\U0000FE00-\U0000FE0F"
    r"\U0001F000-\U0001F02F\U0000200D\U00003299\U00003297\U0000203C"
    r"\U00002049\U000020E3\U00002122\U00002139\U00002194-\U00002199"
    r"\U000021A9-\U000021AA\U0000231A-\U0000231B\U00002328\U000023CF"
    r"\U000023E9-\U000023F3\U000023F8-\U000023FA]+$"
)


def load_high_freq_slugs():
    """Load slugs with 10+ unique authors and their stats."""
    slugs = {}
    with open(CENSUS_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total = int(row["total_comments"])
            authors = int(row["unique_authors_commented_on"])
            if total >= 10:  # 10+ total comments
                slugs[row["commenter_slug"]] = {
                    "name": row["commenter_name"],
                    "unique_authors": authors,
                    "in_registry": row["in_registry"],
                }
    return slugs


def load_comments():
    """Load all comments."""
    comments = []
    path = os.path.join(SCRAPED_DIR, "comments.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            comments.append(row)
    return comments


def is_generic(text):
    """Check if comment text matches generic engagement patterns."""
    text = text.strip()
    if not text:
        return True

    # Emoji-only
    if EMOJI_ONLY.match(text):
        return True

    # Check generic phrases
    for pattern in GENERIC_PATTERNS:
        if pattern.search(text):
            return True

    return False


def is_short(text, threshold=10):
    """Check if comment is under threshold words."""
    words = text.strip().split()
    return len(words) < threshold


def analyze_comments(comments, high_freq_slugs):
    """Analyze comment quality for high-frequency commenters."""
    # Collect comments per slug
    slug_comments = defaultdict(list)
    for c in comments:
        slug = c["comment_author_slug"]
        if slug in high_freq_slugs:
            slug_comments[slug].append(c.get("comment_text", ""))

    rows = []
    for slug, info in high_freq_slugs.items():
        texts = slug_comments.get(slug, [])
        if not texts:
            continue

        total = len(texts)
        short_count = sum(1 for t in texts if is_short(t))
        generic_count = sum(1 for t in texts if is_generic(t))

        # Unique text ratio: distinct texts / total
        normalized = [t.strip().lower() for t in texts if t.strip()]
        unique_texts = len(set(normalized))
        unique_ratio = round(unique_texts / total, 3) if total else 0

        pct_short = round(short_count / total * 100, 1)
        pct_generic = round(generic_count / total * 100, 1)

        # VA flag: high generic % or low uniqueness
        suspected_va = pct_generic > 50 or unique_ratio < 0.3

        rows.append({
            "commenter_slug": slug,
            "name": info["name"],
            "total_comments": total,
            "unique_authors": info["unique_authors"],
            "pct_short_comments": pct_short,
            "pct_generic": pct_generic,
            "unique_text_ratio": unique_ratio,
            "suspected_va": suspected_va,
            "in_registry": info["in_registry"],
        })

    return rows


def write_output(rows):
    """Write quality analysis CSV."""
    rows.sort(key=lambda r: r["pct_generic"], reverse=True)
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    if rows:
        with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
    return rows


def display_results(rows):
    """Print summary."""
    suspected = [r for r in rows if r["suspected_va"]]

    print("=" * 100)
    print("COMMENT TEXT QUALITY ANALYSIS")
    print("=" * 100)
    print(f"\nCommenters analyzed (10+ total comments): {len(rows)}")
    print(f"Suspected VA accounts: {len(suspected)}")

    print(f"\n{'='*100}")
    print("ALL SUSPECTED VA ACCOUNTS")
    print(f"{'='*100}")
    print(f"{'Slug':<40} {'Authors':>7} {'Comments':>8} {'%Generic':>8} {'%Short':>7} {'Uniq':>6} {'Registry'}")
    print("-" * 100)
    for r in sorted(suspected, key=lambda x: x["unique_authors"], reverse=True):
        reg = "YES" if r["in_registry"] == "True" else "---"
        print(f"{r['commenter_slug']:<40} {r['unique_authors']:>7} {r['total_comments']:>8} {r['pct_generic']:>7.1f}% {r['pct_short_comments']:>6.1f}% {r['unique_text_ratio']:>6.3f} {reg}")

    print(f"\n{'='*100}")
    print("TOP 30 BY GENERIC COMMENT PERCENTAGE")
    print(f"{'='*100}")
    print(f"{'Slug':<40} {'Authors':>7} {'Comments':>8} {'%Generic':>8} {'%Short':>7} {'Uniq':>6} {'VA?'}")
    print("-" * 100)
    sorted_generic = sorted(rows, key=lambda x: x["pct_generic"], reverse=True)
    for r in sorted_generic[:30]:
        va = "YES" if r["suspected_va"] else "no"
        print(f"{r['commenter_slug']:<40} {r['unique_authors']:>7} {r['total_comments']:>8} {r['pct_generic']:>7.1f}% {r['pct_short_comments']:>6.1f}% {r['unique_text_ratio']:>6.3f} {va}")


if __name__ == "__main__":
    print("Loading high-frequency commenter list...")
    high_freq = load_high_freq_slugs()
    print(f"Commenters to analyze: {len(high_freq)}")

    print("Loading comments...")
    comments = load_comments()
    print(f"Comments: {len(comments):,}")

    print("Analyzing comment quality...")
    rows = analyze_comments(comments, high_freq)

    print("Writing output...")
    rows = write_output(rows)

    display_results(rows)
    print(f"\nOutput: {OUTPUT}")
