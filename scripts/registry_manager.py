#!/usr/bin/env python3
"""Registry manager for Ghost Sweep investigation.

Usage:
    python scripts/registry_manager.py add-profile --linkedin-url URL --display-name NAME --role ROLE --confidence TIER [options]
    python scripts/registry_manager.py add-evidence --profile-id PID --evidence-type TYPE --evidence-source SRC --timestamp TS --description DESC
    python scripts/registry_manager.py add-network --name NAME --platform PLAT --operated-by OP [options]
    python scripts/registry_manager.py search QUERY
    python scripts/registry_manager.py summary
"""

import argparse
import csv
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REGISTRY_DIR = os.path.join(BASE_DIR, "registry")

PROFILES_CSV = os.path.join(REGISTRY_DIR, "profiles.csv")
EVIDENCE_CSV = os.path.join(REGISTRY_DIR, "evidence-tags.csv")
NETWORKS_CSV = os.path.join(REGISTRY_DIR, "networks.csv")

PROFILE_FIELDS = ["profile_id", "linkedin_url", "display_name", "whatsapp_name",
                   "role", "confidence", "network", "client_of", "notes", "date_added"]
EVIDENCE_FIELDS = ["profile_id", "evidence_type", "evidence_source", "timestamp", "description"]
NETWORK_FIELDS = ["network_id", "name", "platform", "operated_by", "created_date",
                   "discovered_date", "evidence_source", "member_count", "notes"]


def read_csv(path, fields):
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv(path, fields, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


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
    return f"P{max_id + 1:03d}"


def next_network_id(rows):
    max_id = 0
    for row in rows:
        nid = row.get("network_id", "")
        if nid.startswith("N"):
            try:
                num = int(nid[1:])
                max_id = max(max_id, num)
            except ValueError:
                pass
    return f"N{max_id + 1:03d}"


def cmd_add_profile(args):
    from datetime import date
    rows = read_csv(PROFILES_CSV, PROFILE_FIELDS)
    pid = next_profile_id(rows)
    new_row = {
        "profile_id": pid,
        "linkedin_url": args.linkedin_url,
        "display_name": args.display_name,
        "whatsapp_name": args.whatsapp_name or "",
        "role": args.role,
        "confidence": args.confidence,
        "network": args.network or "",
        "client_of": args.client_of or "",
        "notes": args.notes or "",
        "date_added": str(date.today()),
    }
    rows.append(new_row)
    write_csv(PROFILES_CSV, PROFILE_FIELDS, rows)
    print(f"Added {pid}: {args.display_name} ({args.role}, {args.confidence})")


def cmd_add_evidence(args):
    rows = read_csv(EVIDENCE_CSV, EVIDENCE_FIELDS)
    new_row = {
        "profile_id": args.profile_id,
        "evidence_type": args.evidence_type,
        "evidence_source": args.evidence_source,
        "timestamp": args.timestamp,
        "description": args.description,
    }
    rows.append(new_row)
    write_csv(EVIDENCE_CSV, EVIDENCE_FIELDS, rows)
    print(f"Added evidence for {args.profile_id}: {args.description[:60]}")


def cmd_add_network(args):
    from datetime import date
    rows = read_csv(NETWORKS_CSV, NETWORK_FIELDS)
    nid = next_network_id(rows)
    new_row = {
        "network_id": nid,
        "name": args.name,
        "platform": args.platform,
        "operated_by": args.operated_by,
        "created_date": args.created_date or "",
        "discovered_date": args.discovered_date or str(date.today()),
        "evidence_source": args.evidence_source or "",
        "member_count": args.member_count or "",
        "notes": args.notes or "",
    }
    rows.append(new_row)
    write_csv(NETWORKS_CSV, NETWORK_FIELDS, rows)
    print(f"Added {nid}: {args.name} on {args.platform}")


def cmd_search(args):
    query = args.query.lower()
    rows = read_csv(PROFILES_CSV, PROFILE_FIELDS)
    matches = []
    for row in rows:
        searchable = " ".join(row.values()).lower()
        if query in searchable:
            matches.append(row)
    if not matches:
        print(f"No profiles matching '{args.query}'")
        return
    print(f"Found {len(matches)} profile(s) matching '{args.query}':\n")
    for row in matches:
        print(f"  {row['profile_id']}: {row['display_name']}")
        print(f"    URL: {row['linkedin_url']}")
        print(f"    Role: {row['role']} | Confidence: {row['confidence']} | Network: {row['network']}")
        if row.get("client_of"):
            print(f"    Client of: {row['client_of']}")
        if row.get("notes"):
            print(f"    Notes: {row['notes']}")
        print()


def cmd_summary(args):
    rows = read_csv(PROFILES_CSV, PROFILE_FIELDS)
    if not rows:
        print("No profiles in registry.")
        return

    print(f"Total profiles: {len(rows)}\n")

    # By role
    roles = {}
    for row in rows:
        role = row.get("role", "unknown")
        roles[role] = roles.get(role, 0) + 1
    print("By role:")
    for role, count in sorted(roles.items(), key=lambda x: -x[1]):
        print(f"  {role}: {count}")

    # By confidence
    tiers = {}
    for row in rows:
        tier = row.get("confidence", "unknown")
        tiers[tier] = tiers.get(tier, 0) + 1
    print("\nBy confidence tier:")
    for tier, count in sorted(tiers.items(), key=lambda x: -x[1]):
        print(f"  {tier}: {count}")

    # By network
    networks = {}
    for row in rows:
        net = row.get("network", "") or "unassigned"
        networks[net] = networks.get(net, 0) + 1
    print("\nBy network:")
    for net, count in sorted(networks.items(), key=lambda x: -x[1]):
        print(f"  {net}: {count}")


def main():
    parser = argparse.ArgumentParser(description="Ghost Sweep Registry Manager")
    subparsers = parser.add_subparsers(dest="command")

    # add-profile
    ap = subparsers.add_parser("add-profile", help="Add a new profile")
    ap.add_argument("--linkedin-url", required=True)
    ap.add_argument("--display-name", required=True)
    ap.add_argument("--role", required=True, choices=["operator", "va", "va|member", "member", "client", "non-participant", "unknown"])
    ap.add_argument("--confidence", required=True, choices=["confirmed", "probable", "lead"])
    ap.add_argument("--whatsapp-name", default="")
    ap.add_argument("--network", default="")
    ap.add_argument("--client-of", default="")
    ap.add_argument("--notes", default="")

    # add-evidence
    ae = subparsers.add_parser("add-evidence", help="Add an evidence tag")
    ae.add_argument("--profile-id", required=True)
    ae.add_argument("--evidence-type", required=True)
    ae.add_argument("--evidence-source", required=True)
    ae.add_argument("--timestamp", required=True)
    ae.add_argument("--description", required=True)

    # add-network
    an = subparsers.add_parser("add-network", help="Add a network")
    an.add_argument("--name", required=True)
    an.add_argument("--platform", required=True)
    an.add_argument("--operated-by", required=True)
    an.add_argument("--created-date", default="")
    an.add_argument("--discovered-date", default="")
    an.add_argument("--evidence-source", default="")
    an.add_argument("--member-count", default="")
    an.add_argument("--notes", default="")

    # search
    sp = subparsers.add_parser("search", help="Search profiles")
    sp.add_argument("query", help="Search term (name, URL, or role)")

    # summary
    subparsers.add_parser("summary", help="Print registry summary")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "add-profile": cmd_add_profile,
        "add-evidence": cmd_add_evidence,
        "add-network": cmd_add_network,
        "search": cmd_search,
        "summary": cmd_summary,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
