#!/usr/bin/env python3
"""Task 7: Ring membership tiers + Task 8: Mass registry update."""

import csv
import os
import sys
from collections import defaultdict
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COMMENTS_CSV = os.path.join(BASE_DIR, "data", "scraped", "comments.csv")
PROFILES_CSV = os.path.join(BASE_DIR, "registry", "profiles.csv")
MASTER_CSV = os.path.join(BASE_DIR, "data", "analysis", "master-ring-members.csv")
RECIPROCAL_CSV = os.path.join(BASE_DIR, "data", "analysis", "reciprocal-pairs.csv")
BURST_CSV = os.path.join(BASE_DIR, "data", "analysis", "burst-events.csv")
VA_CSV = os.path.join(BASE_DIR, "data", "analysis", "va-identification.csv")
CLUSTERS_CSV = os.path.join(BASE_DIR, "data", "analysis", "clusters.csv")
FULL_CLUSTERS_CSV = os.path.join(BASE_DIR, "data", "analysis", "full-network-clusters.csv")
GP_TARGETS_CSV = os.path.join(BASE_DIR, "data", "analysis", "game-plan-targets.csv")
OUT_TIERS = os.path.join(BASE_DIR, "data", "analysis", "ring-tiers.csv")

PROFILE_FIELDS = ["profile_id", "linkedin_url", "display_name", "whatsapp_name",
                   "role", "confidence", "network", "client_of", "notes", "date_added"]

# Tier 1: known operators with direct evidence
TIER1_SLUGS = {"coryblumenfeld", "charlie-hills"}

# WhatsApp members (P001-P068 in the original growth-community network)


def load_registry():
    reg = {}
    rows = []
    with open(PROFILES_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slug = row["linkedin_url"].rstrip("/").split("/")[-1]
            reg[slug] = row
            rows.append(row)
    return reg, rows


def load_master():
    members = {}
    with open(MASTER_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            members[row["slug"]] = row
    return members


def load_reciprocal_pairs():
    pairs = defaultdict(lambda: defaultdict(int))
    with open(RECIPROCAL_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            a, b = row["profile_a"], row["profile_b"]
            a_to_b = int(row["a_to_b_count"])
            b_to_a = int(row["b_to_a_count"])
            pairs[a][b] = min(a_to_b, b_to_a)
            pairs[b][a] = min(a_to_b, b_to_a)
    return pairs


def load_burst_counts():
    counts = defaultdict(int)
    with open(BURST_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            slugs = row["commenter_slugs"].split(";")
            for slug in slugs:
                counts[slug.strip()] += 1
    return counts


def load_va_scores():
    scores = {}
    with open(VA_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            scores[row["slug"]] = int(row["va_score"])
    return scores


def load_clusters():
    clusters = {}
    # Try full network clusters first
    for path in [FULL_CLUSTERS_CSV, CLUSTERS_CSV]:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    slug = row.get("slug", row.get("commenter_slug", ""))
                    if slug:
                        clusters[slug] = row.get("cluster_id", "")
    return clusters


def load_gp_targets():
    targets = {}
    with open(GP_TARGETS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            targets[row["author_slug"]] = int(row["comment_count"])
    return targets


def next_profile_id(rows):
    max_id = 0
    for row in rows:
        pid = row.get("profile_id", "")
        if pid.startswith("P"):
            try:
                num = int(pid[1:])
                max_id = max(max_id, num)
            except ValueError:
                pass
    return max_id


def main():
    print("Loading data...")
    registry, reg_rows = load_registry()
    master = load_master()
    reciprocal = load_reciprocal_pairs()
    burst_counts = load_burst_counts()
    va_scores = load_va_scores()
    clusters = load_clusters()
    gp_targets = load_gp_targets()

    # Build commenter->author indexes from comments
    commenter_authors = defaultdict(set)
    author_names = {}
    commenter_urls = {}
    with open(COMMENTS_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            commenter_authors[row["comment_author_slug"]].add(row["post_author_slug"])
            if row["comment_author_slug"] not in author_names:
                author_names[row["comment_author_slug"]] = row["comment_author_name"]
                commenter_urls[row["comment_author_slug"]] = row["comment_author_linkedin_url"]

    # Get confirmed member slugs (for reciprocal counting)
    confirmed_slugs = set()
    whatsapp_slugs = set()
    for slug, r in registry.items():
        if r["confidence"] == "confirmed":
            confirmed_slugs.add(slug)
        if r["network"] in ("growth-community", "linked-agency"):
            if r["confidence"] == "confirmed":
                whatsapp_slugs.add(slug)

    print(f"  {len(master)} master ring members, {len(registry)} registry, "
          f"{len(confirmed_slugs)} confirmed, {len(reciprocal)} with reciprocal pairs")

    # Tier assignment
    gp_target_slugs = set(gp_targets.keys())
    tier_results = []

    all_slugs = set(master.keys()) | set(registry.keys())

    for slug in all_slugs:
        m = master.get(slug, {})
        r = registry.get(slug, {})

        name = m.get("name", "") or r.get("display_name", "") or author_names.get(slug, "")
        url = m.get("linkedin_url", "") or r.get("linkedin_url", "") or commenter_urls.get(slug, "")
        total_comments = int(m.get("total_comments_in_dataset", 0))
        unique_authors = int(m.get("unique_authors", 0))

        # Count reciprocal pairs with confirmed members (5+ each direction)
        recip_confirmed_5 = 0
        recip_confirmed_3 = 0
        for partner, min_count in reciprocal.get(slug, {}).items():
            if partner in confirmed_slugs:
                if min_count >= 5:
                    recip_confirmed_5 += 1
                if min_count >= 3:
                    recip_confirmed_3 += 1

        # Total reciprocal pairs
        total_recip = len(reciprocal.get(slug, {}))

        va_score = va_scores.get(slug, 0)
        burst_count = burst_counts.get(slug, 0)
        in_reg = slug in registry
        reg_role = r.get("role", "")
        reg_confidence = r.get("confidence", "")
        reg_network = r.get("network", "")
        cluster_id = clusters.get(slug, "")

        # Ring member overlap
        ring_overlap = len(commenter_authors.get(slug, set()) & gp_target_slugs)

        # Determine tier
        tier = 6
        tier_reason = ""

        # TIER 1: Confirmed operator
        if slug in TIER1_SLUGS:
            tier = 1
            tier_reason = "Direct evidence: call recording, email, WhatsApp admin"

        # TIER 2: Confirmed participant (WhatsApp export OR VA-client proven)
        elif reg_confidence == "confirmed" and reg_network in ("growth-community", "linked-agency", "growth-community-extended"):
            tier = 2
            tier_reason = f"Confirmed participant in {reg_network}"
        elif reg_role == "client" and reg_confidence == "confirmed":
            tier = 2
            tier_reason = f"Confirmed client relationship"

        # TIER 3: Proven by data
        elif recip_confirmed_5 >= 5:
            tier = 3
            tier_reason = f"Reciprocal 5+ with {recip_confirmed_5} confirmed members"
        elif burst_count >= 10:
            tier = 3
            tier_reason = f"{burst_count} burst event appearances"
        elif va_score >= 3:
            tier = 3
            tier_reason = f"VA score {va_score}"
        elif ring_overlap >= 30 and total_comments >= 100:
            tier = 3
            tier_reason = f"Comments on {ring_overlap}/68 GP targets with {total_comments} total comments"

        # TIER 4: Strong indicator
        elif ring_overlap >= 20:
            tier = 4
            tier_reason = f"Comments on {ring_overlap}/68 GP targets"
        elif recip_confirmed_3 >= 3:
            tier = 4
            tier_reason = f"Reciprocal 3+ with {recip_confirmed_3} confirmed members"
        elif va_score >= 2:
            tier = 4
            tier_reason = f"VA score {va_score}"
        elif unique_authors >= 20 and burst_count >= 5:
            tier = 4
            tier_reason = f"{unique_authors} unique authors, {burst_count} burst events"

        # TIER 5: Suspected
        elif ring_overlap >= 10:
            tier = 5
            tier_reason = f"Comments on {ring_overlap}/68 GP targets"
        elif recip_confirmed_3 >= 1:
            tier = 5
            tier_reason = f"Reciprocal 3+ with {recip_confirmed_3} confirmed members"
        elif unique_authors >= 15 and burst_count >= 3:
            tier = 5
            tier_reason = f"{unique_authors} unique authors, {burst_count} burst events"

        # TIER 6: Lead
        elif ring_overlap >= 5 or unique_authors >= 10:
            tier = 6
            tier_reason = f"Comments on {ring_overlap} GP targets, {unique_authors} unique authors"
        else:
            tier = 6
            tier_reason = f"Low signal: {ring_overlap} GP targets, {unique_authors} authors"

        # Scrape priority
        if tier <= 2:
            scrape_priority = "scraped" if in_reg else "critical"
        elif tier == 3:
            scrape_priority = "critical"
        elif tier == 4:
            scrape_priority = "high"
        elif tier == 5:
            scrape_priority = "medium"
        else:
            scrape_priority = "low"

        tier_results.append({
            "slug": slug,
            "name": name,
            "linkedin_url": url,
            "tier": tier,
            "tier_reason": tier_reason,
            "total_comments": total_comments,
            "unique_authors": unique_authors,
            "reciprocal_pairs": total_recip,
            "va_score": va_score,
            "burst_appearances": burst_count,
            "in_registry": in_reg,
            "scrape_priority": scrape_priority,
        })

    tier_results.sort(key=lambda x: (x["tier"], -x["total_comments"]))

    with open(OUT_TIERS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "slug", "name", "linkedin_url", "tier", "tier_reason", "total_comments",
            "unique_authors", "reciprocal_pairs", "va_score", "burst_appearances",
            "in_registry", "scrape_priority"
        ])
        writer.writeheader()
        writer.writerows(tier_results)

    print(f"Wrote {len(tier_results)} tier assignments to {OUT_TIERS}")

    # Display tier counts
    tier_counts = defaultdict(int)
    for r in tier_results:
        tier_counts[r["tier"]] += 1

    tier_labels = {
        1: "CONFIRMED OPERATOR",
        2: "CONFIRMED PARTICIPANT",
        3: "PROVEN BY DATA",
        4: "STRONG INDICATOR",
        5: "SUSPECTED",
        6: "LEAD",
    }

    print(f"\n{'=' * 80}")
    print("RING MEMBERSHIP TIERS")
    print(f"{'=' * 80}")
    for t in range(1, 7):
        print(f"  Tier {t} ({tier_labels[t]}): {tier_counts[t]}")
    print(f"  Total: {len(tier_results)}")

    # Display Tiers 1-3 (publishable)
    publishable = [r for r in tier_results if r["tier"] <= 3]
    print(f"\n{'=' * 120}")
    print(f"TIERS 1-3: PUBLISHABLE EVIDENCE ({len(publishable)} accounts)")
    print(f"{'=' * 120}")
    print(f"{'T':<3} {'Slug':<38} {'Name':<28} {'Cmts':<6} {'Auth':<5} {'Recip':<6} {'VA':<4} {'Burst':<6} {'Reason'}")
    print("-" * 120)
    for r in publishable:
        name = r["name"][:26] if r["name"] else ""
        print(f"{r['tier']:<3} {r['slug']:<38} {name:<28} {r['total_comments']:<6} "
              f"{r['unique_authors']:<5} {r['reciprocal_pairs']:<6} {r['va_score']:<4} "
              f"{r['burst_appearances']:<6} {r['tier_reason'][:50]}")

    # Task 8: Mass registry update
    if "--register" in sys.argv:
        print(f"\n{'=' * 80}")
        print("MASS REGISTRY UPDATE")
        print(f"{'=' * 80}")

        current_max = next_profile_id(reg_rows)
        new_registrations = 0
        tier3_added = 0
        tier4_added = 0

        today = str(date.today())
        new_rows = list(reg_rows)

        for r in tier_results:
            slug = r["slug"]
            if slug in registry:
                continue

            if r["tier"] == 3:
                current_max += 1
                pid = f"P{current_max:03d}"
                cluster = clusters.get(slug, "")
                network = f"cluster-{cluster}" if cluster else "extended-network"
                new_row = {
                    "profile_id": pid,
                    "linkedin_url": r["linkedin_url"],
                    "display_name": r["name"],
                    "whatsapp_name": "",
                    "role": "member",
                    "confidence": "confirmed",
                    "network": network,
                    "client_of": "",
                    "notes": f"Tier 3: {r['tier_reason']}",
                    "date_added": today,
                }
                new_rows.append(new_row)
                registry[slug] = new_row
                new_registrations += 1
                tier3_added += 1

            elif r["tier"] == 4:
                current_max += 1
                pid = f"P{current_max:03d}"
                cluster = clusters.get(slug, "")
                network = f"cluster-{cluster}" if cluster else "extended-network"
                new_row = {
                    "profile_id": pid,
                    "linkedin_url": r["linkedin_url"],
                    "display_name": r["name"],
                    "whatsapp_name": "",
                    "role": "member",
                    "confidence": "probable",
                    "network": network,
                    "client_of": "",
                    "notes": f"Tier 4: {r['tier_reason']}",
                    "date_added": today,
                }
                new_rows.append(new_row)
                registry[slug] = new_row
                new_registrations += 1
                tier4_added += 1

        # Write updated registry (clean any extra keys from CSV parsing)
        cleaned_rows = []
        for row in new_rows:
            cleaned = {k: row.get(k, "") for k in PROFILE_FIELDS}
            cleaned_rows.append(cleaned)
        with open(PROFILES_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=PROFILE_FIELDS)
            writer.writeheader()
            writer.writerows(cleaned_rows)

        print(f"  Tier 3 new registrations (confirmed): {tier3_added}")
        print(f"  Tier 4 new registrations (probable): {tier4_added}")
        print(f"  Total new registrations: {new_registrations}")
        print(f"  Updated registry count: {len(new_rows)}")
    else:
        # Preview what would be registered
        tier3_unreg = [r for r in tier_results if r["tier"] == 3 and not r["in_registry"]]
        tier4_unreg = [r for r in tier_results if r["tier"] == 4 and not r["in_registry"]]
        print(f"\nREGISTRY UPDATE PREVIEW (run with --register to execute):")
        print(f"  Tier 3 not in registry: {len(tier3_unreg)} (would register as confirmed)")
        print(f"  Tier 4 not in registry: {len(tier4_unreg)} (would register as probable)")
        print(f"  Current registry: {len(registry)}")
        print(f"  Projected registry: {len(registry) + len(tier3_unreg) + len(tier4_unreg)}")


if __name__ == "__main__":
    main()
