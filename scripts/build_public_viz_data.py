#!/usr/bin/env python3
"""Build public visualization data.json from analysis files.

Reads all analysis CSVs and outputs a single JSON file with anonymized names
suitable for the public-facing interactive visualization.

All claims must be provable from public LinkedIn data alone.
No references to WhatsApp, call recordings, emails, VAs, or company names.
"""

import csv
import json
import os
import re
import sys
from collections import defaultdict

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ANALYSIS = os.path.join(BASE, "data", "analysis")
OUTPUT = os.path.join(BASE, "output", "visualization")

SUFFIX_LIST = [
    "PhD", "Ph.D", "Ph.D.", "PsyD", "Psy.D", "Psy.D.", "DBA", "D.B.A",
    "MBA", "M.Sc", "MSc", "MPA", "MPH", "PharmD",
    "MD", "M.D", "M.D.", "DO", "D.O", "JD", "J.D", "LLM", "LL.M", "EdD", "Ed.D",
    "CFA", "CPA", "PMP", "LSSBB", "CSM", "CSPO", "ACC", "PCC", "MCC", "ICF",
    "FCCA", "PROSCI", "SHRM", "SPHR", "PHR", "CFP", "ChFC", "CLU", "CISA", "CISSP",
    "RN", "BSN", "DNP", "FNP", "PA-C", "OD", "DDS", "DMD", "DPT", "DC",
    "PE", "SE", "AIA", "FAIA", "LEED", "WELL",
    "Esq", "Ret", "USAF", "USN", "USMC", "USA",
    "MGM", "EMBA", "BOD", "IMD", "CCHT",
    "FICS", "FNI", "SR.MIEEE", "SR.MIES",
    "Jr", "Jr.", "Sr", "Sr.", "III", "II",
]

EMOJI_RE = re.compile(
    r"[\U0001F300-\U0001F9FF\U00002600-\U000027BF\U0000FE00-\U0000FE0F"
    r"\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002702-\U000027B0"
    r"\U0000200D\U0000FE0F]+",
    re.UNICODE,
)


def strip_suffixes(name):
    """Remove professional/academic suffixes from a name."""
    # Build a set for fast lookup (case-insensitive)
    suffix_lower = {s.lower().rstrip(".") for s in SUFFIX_LIST}

    # Remove parenthesized content like (FICS, FNI)
    name = re.sub(r"\s*\([^)]*\)", "", name)

    # Split on commas first, check if trailing parts are suffixes
    parts = [p.strip() for p in name.split(",")]
    clean_parts = [parts[0]]
    for part in parts[1:]:
        # Check if this comma-separated part is all suffixes
        tokens = part.split()
        all_suffix = all(
            t.lower().rstrip(".®™") in suffix_lower or re.match(r"^[A-Z][A-Z.®™]+$", t)
            for t in tokens
        ) if tokens else True
        if not all_suffix:
            clean_parts.append(part)

    name = ", ".join(clean_parts).strip().rstrip(",").strip()
    return name


def anonymize_name(full_name):
    """Convert full name to 'First L.' format for public display."""
    if not full_name or not full_name.strip():
        return "Unknown"

    name = full_name.strip()

    # Company/page accounts: truncate at dash/hyphen (surrounded by spaces)
    if " - " in name:
        name = name.split(" - ")[0].strip()
        return name

    # Strip emojis and special unicode
    name = EMOJI_RE.sub("", name).strip()
    # Strip superscript characters
    name = re.sub(r"[\u1d2c-\u1d6a\u2070-\u209f]+", "", name).strip()

    # Handle phone numbers or non-name strings
    if name.startswith("+") or name.startswith("\u202a"):
        return "Anonymous"

    # Strip suffixes
    name = strip_suffixes(name)

    # Strip trailing commas/periods/spaces
    name = name.rstrip(".,").strip()

    # Handle quoted nicknames like 'Pree'
    name = re.sub(r"\s*['\u2018\u2019][^'\u2018\u2019]+['\u2018\u2019]\s*", " ", name).strip()

    parts = name.split()
    if not parts:
        return "Unknown"

    if len(parts) == 1:
        return parts[0]

    # Preserve prefix (Dr., Prof., Capt.)
    prefix = ""
    if parts[0].rstrip(".") in ("Dr", "Prof", "Rev", "Sir", "Capt"):
        prefix = parts[0] + " "
        parts = parts[1:]

    if not parts:
        return prefix.strip()

    if len(parts) == 1:
        return prefix + parts[0]

    first = parts[0]
    last_initial = parts[-1][0].upper() + "."

    return prefix + first + " " + last_initial


def read_csv(filename):
    path = os.path.join(ANALYSIS, filename)
    if not os.path.exists(path):
        print(f"  WARNING: {filename} not found")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def to_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def to_float(val, default=0.0):
    try:
        return round(float(val), 1)
    except (ValueError, TypeError):
        return default


def make_slug_to_name(ring_tiers):
    """Build slug -> anonymized name mapping."""
    mapping = {}
    for row in ring_tiers:
        mapping[row["slug"]] = anonymize_name(row["name"])
    return mapping


def build_nodes(ring_tiers, clusters_data, burst_data, timing_data, quality_data, va_data):
    """Build node list from Tier 1-5 accounts."""
    # Build cluster lookup
    cluster_lookup = {}
    for row in clusters_data:
        cluster_lookup[row["slug"]] = to_int(row["cluster_id"])

    # Build burst count per slug
    burst_counts = defaultdict(int)
    for row in burst_data:
        slugs = row["commenter_slugs"].split(";")
        for s in slugs:
            burst_counts[s] += 1

    # Build timing lookup
    timing_lookup = {}
    for row in timing_data:
        timing_lookup[row["commenter_slug"]] = to_float(row["avg_response_minutes"])

    # Build quality lookup
    quality_lookup = {}
    for row in quality_data:
        quality_lookup[row["commenter_slug"]] = to_float(row["pct_generic"])

    # Build VA score lookup
    va_lookup = {}
    for row in va_data:
        va_lookup[row["slug"]] = to_int(row["va_score"])

    tier_labels = {
        1: "Network Operator",
        2: "Core Participant",
        3: "Proven Participant",
        4: "Strong Pattern Match",
        5: "Suspected Participant",
    }

    nodes = []
    for row in ring_tiers:
        tier = to_int(row["tier"])
        if tier < 1 or tier > 5:
            continue

        slug = row["slug"]
        va_score = va_lookup.get(slug, to_int(row.get("va_score", 0)))

        role = "operator" if tier == 1 else ("automated" if va_score >= 2 else "participant")

        node = {
            "id": slug,
            "name": anonymize_name(row["name"]),
            "linkedin_url": row["linkedin_url"],
            "tier": tier,
            "tier_label": tier_labels[tier],
            "role": role,
            "cluster_id": cluster_lookup.get(slug, None),
            "metrics": {
                "total_comments": to_int(row["total_comments"]),
                "unique_authors": to_int(row["unique_authors"]),
                "reciprocal_pairs": to_int(row["reciprocal_pairs"]),
                "pct_generic_comments": quality_lookup.get(slug, 0.0),
                "burst_appearances": burst_counts.get(slug, to_int(row.get("burst_appearances", 0))),
                "avg_response_minutes": timing_lookup.get(slug, 0.0),
            },
        }
        nodes.append(node)

    return nodes


def build_edges(pairs_data, tier_lookup, slug_names):
    """Build edges from full-network-pairs.csv, both endpoints Tier 1-5, count >= 5."""
    edges = []
    pair_set = set()
    for row in pairs_data:
        src = row["commenter_slug"]
        tgt = row["author_slug"]
        count = to_int(row["comment_count"])

        if count < 5:
            continue
        if src not in tier_lookup or tgt not in tier_lookup:
            continue
        if tier_lookup[src] > 5 or tier_lookup[tgt] > 5:
            continue

        pair_set.add((src, tgt))
        edges.append({
            "source": src,
            "target": tgt,
            "weight": count,
            "reciprocal": False,  # will be filled in second pass
            "source_name": slug_names.get(src, src),
            "target_name": slug_names.get(tgt, tgt),
        })

    # Mark reciprocal edges
    for edge in edges:
        reverse = (edge["target"], edge["source"])
        if reverse in pair_set:
            edge["reciprocal"] = True

    return edges


def build_reciprocal_pairs(recip_data, slug_names):
    """Build reciprocal pairs list."""
    pairs = []
    for row in recip_data:
        a = row["profile_a"]
        b = row["profile_b"]
        pairs.append({
            "a": a,
            "b": b,
            "a_name": slug_names.get(a, anonymize_name(a)),
            "b_name": slug_names.get(b, anonymize_name(b)),
            "a_to_b": to_int(row["a_to_b_count"]),
            "b_to_a": to_int(row["b_to_a_count"]),
            "total": to_int(row["total"]),
        })
    return pairs


def build_bursts(burst_data, slug_names):
    """Build top 200 burst events."""
    bursts = []
    for row in burst_data:
        commenter_slugs = row["commenter_slugs"].split(";")
        commenters = []
        for s in commenter_slugs:
            commenters.append({
                "slug": s,
                "name": slug_names.get(s, anonymize_name(s)),
            })

        # Extract date from timestamp
        ts = row.get("burst_timestamp", "")
        date = ts[:10] if ts else None

        bursts.append({
            "post_author": row["post_author"],
            "post_author_name": slug_names.get(row["post_author"], anonymize_name(row["post_author"])),
            "commenter_count": to_int(row["commenters_in_burst"]),
            "window_minutes": to_float(row["burst_window_minutes"]),
            "commenters": commenters,
            "date": date,
        })

    # Sort by commenter count desc, take top 200
    bursts.sort(key=lambda x: x["commenter_count"], reverse=True)
    return bursts[:200]


def build_clusters(clusters_data, cluster_summary, slug_names):
    """Build cluster info."""
    clusters = []
    # Build member lists per cluster
    members_by_cluster = defaultdict(list)
    for row in clusters_data:
        cid = to_int(row["cluster_id"])
        members_by_cluster[cid].append(row["slug"])

    for row in cluster_summary:
        cid = to_int(row["cluster_id"])
        hub = row["hub_account"]
        clusters.append({
            "id": cid,
            "member_count": to_int(row["member_count"]),
            "internal_pairs": to_int(row["internal_pairs"]),
            "density": to_float(row["avg_weight"]),
            "hub_slug": hub,
            "hub_name": slug_names.get(hub, anonymize_name(row["hub_name"])),
            "members": members_by_cluster.get(cid, []),
        })
    return clusters


def build_automated_accounts(va_data, slug_names, burst_data):
    """Build automated accounts list (va_score >= 2)."""
    burst_counts = defaultdict(int)
    for row in burst_data:
        for s in row["commenter_slugs"].split(";"):
            burst_counts[s] += 1

    accounts = []
    for row in va_data:
        score = to_int(row["va_score"])
        if score < 2:
            continue

        slug = row["slug"]
        signals = []
        flags = row.get("signal_flags", "")
        if "pct_generic" in flags:
            signals.append("HIGH GENERIC %")
        if "avg_response" in flags:
            signals.append("RAPID RESPONSE")
        if "burst_count" in flags:
            signals.append("BURST PATTERN")
        if "all_operators" in flags:
            signals.append("CROSS-NETWORK")
        if to_int(row.get("unique_authors", 0)) >= 15:
            signals.append("HIGH VOLUME")

        accounts.append({
            "slug": slug,
            "name": slug_names.get(slug, anonymize_name(row["name"])),
            "linkedin_url": row["linkedin_url"],
            "score": score,
            "signals": signals,
            "total_comments": to_int(row["total_comments"]),
            "unique_authors": to_int(row["unique_authors"]),
            "pct_generic": to_float(row["pct_generic"]),
            "burst_appearances": burst_counts.get(slug, to_int(row.get("burst_count", 0))),
        })

    accounts.sort(key=lambda x: x["score"], reverse=True)
    return accounts


def build_company_page(game_plan_data, slug_names):
    """Build The Game Plan company page data."""
    targets = []
    total_comments = 0
    for row in game_plan_data:
        slug = row["author_slug"]
        count = to_int(row["comment_count"])
        total_comments += count
        targets.append({
            "slug": slug,
            "name": slug_names.get(slug, anonymize_name(row["author_name"])),
            "comments": count,
        })

    return {
        "slug": "posts",
        "name": "The Game Plan",
        "total_comments": total_comments,
        "targets": targets,
    }


def build_stats(nodes, edges, recip_pairs, bursts, automated, clusters):
    """Build summary stats."""
    tier_breakdown = defaultdict(int)
    for n in nodes:
        tier_breakdown[n["tier"]] += 1

    recip_weights = [p["total"] for p in recip_pairs]
    burst_sizes = [b["commenter_count"] for b in bursts]

    return {
        "total_accounts": len(nodes),
        "total_comments_analyzed": 60423,
        "total_reciprocal_pairs": len(recip_pairs),
        "total_burst_events": 1453,
        "automated_accounts_count": len(automated),
        "cluster_count": len(clusters),
        "tier_breakdown": dict(tier_breakdown),
        "avg_reciprocal_weight": round(sum(recip_weights) / max(len(recip_weights), 1), 1),
        "max_burst_size": max(burst_sizes) if burst_sizes else 0,
    }


def sanitize_tier_reasons(data):
    """Remove any private evidence references from tier_reason fields."""
    private_terms = [
        "WhatsApp", "whatsapp", "call recording", "email",
        "VA", "GoLogin", "BlueMoso", "Linked Agency",
        "Philippines", "anti-detect",
    ]
    for row in data:
        reason = row.get("tier_reason", "")
        for term in private_terms:
            if term.lower() in reason.lower():
                # Replace with public-safe version
                if row.get("tier") in ("1", 1):
                    row["tier_reason"] = "Network operator identified through engagement pattern analysis"
                elif row.get("tier") in ("2", 2):
                    row["tier_reason"] = "Core participant with extensive network engagement"
                else:
                    row["tier_reason"] = "Identified through statistical engagement analysis"
    return data


def main():
    print("=" * 60)
    print("GHOST SWEEP - Public Visualization Data Builder")
    print("=" * 60)

    # Read all data files
    print("\nReading data files...")
    ring_tiers = read_csv("ring-tiers.csv")
    print(f"  ring-tiers.csv: {len(ring_tiers)} rows")

    pairs = read_csv("full-network-pairs.csv")
    print(f"  full-network-pairs.csv: {len(pairs)} rows")

    recip = read_csv("reciprocal-pairs.csv")
    print(f"  reciprocal-pairs.csv: {len(recip)} rows")

    bursts = read_csv("burst-events.csv")
    print(f"  burst-events.csv: {len(bursts)} rows")

    clusters_data = read_csv("clusters.csv")
    print(f"  clusters.csv: {len(clusters_data)} rows")

    cluster_summary = read_csv("cluster-summary.csv")
    print(f"  cluster-summary.csv: {len(cluster_summary)} rows")

    quality = read_csv("comment-quality.csv")
    print(f"  comment-quality.csv: {len(quality)} rows")

    va = read_csv("va-identification.csv")
    print(f"  va-identification.csv: {len(va)} rows")

    timing = read_csv("comment-timing.csv")
    print(f"  comment-timing.csv: {len(timing)} rows")

    game_plan = read_csv("game-plan-targets.csv")
    print(f"  game-plan-targets.csv: {len(game_plan)} rows")

    # Sanitize private evidence references
    ring_tiers = sanitize_tier_reasons(ring_tiers)

    # Build slug -> name mapping
    slug_names = make_slug_to_name(ring_tiers)
    # Also add game plan targets not in ring-tiers
    for row in game_plan:
        if row["author_slug"] not in slug_names:
            slug_names[row["author_slug"]] = anonymize_name(row["author_name"])

    # Build tier lookup
    tier_lookup = {}
    for row in ring_tiers:
        tier_lookup[row["slug"]] = to_int(row["tier"])

    # Build all sections
    print("\nBuilding nodes...")
    nodes = build_nodes(ring_tiers, clusters_data, bursts, timing, quality, va)
    print(f"  {len(nodes)} nodes (Tier 1-5)")

    print("Building edges...")
    edges = build_edges(pairs, tier_lookup, slug_names)
    print(f"  {len(edges)} edges (count >= 5, both Tier 1-5)")

    print("Building reciprocal pairs...")
    recip_pairs = build_reciprocal_pairs(recip, slug_names)
    print(f"  {len(recip_pairs)} reciprocal pairs")

    print("Building burst events...")
    burst_list = build_bursts(bursts, slug_names)
    print(f"  {len(burst_list)} burst events (top 200)")

    print("Building clusters...")
    cluster_list = build_clusters(clusters_data, cluster_summary, slug_names)
    print(f"  {len(cluster_list)} clusters")

    print("Building automated accounts...")
    automated = build_automated_accounts(va, slug_names, bursts)
    print(f"  {len(automated)} automated accounts (score >= 2)")

    print("Building company page data...")
    company = build_company_page(game_plan, slug_names)
    print(f"  {company['total_comments']} comments on {len(company['targets'])} targets")

    print("Building stats...")
    stats = build_stats(nodes, edges, recip_pairs, burst_list, automated, cluster_list)

    # Assemble output
    output = {
        "nodes": nodes,
        "edges": edges,
        "reciprocal_pairs": recip_pairs,
        "bursts": burst_list,
        "clusters": cluster_list,
        "automated_accounts": automated,
        "company_page": company,
        "stats": stats,
    }

    # Write output
    os.makedirs(OUTPUT, exist_ok=True)
    outpath = os.path.join(OUTPUT, "data.json")
    with open(outpath, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    size = os.path.getsize(outpath)
    print(f"\n{'=' * 60}")
    print(f"OUTPUT: {outpath}")
    print(f"  Nodes: {len(nodes)}")
    print(f"  Edges: {len(edges)}")
    print(f"  File size: {size:,} bytes ({size / 1024:.1f} KB)")
    print(f"\nSample anonymized names:")
    samples = [n["name"] for n in nodes[:10]]
    for s in samples[:5]:
        print(f"  {s}")
    print(f"\nTier breakdown:")
    for tier in sorted(stats["tier_breakdown"]):
        print(f"  Tier {tier}: {stats['tier_breakdown'][tier]}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
