PART 0: CREATE CLAUDE.md

Create CLAUDE.md at the project root with the following content:

# Ghost Sweep Investigation

## What This Project Is

Data-driven investigation into coordinated LinkedIn engagement fraud operated by Linked Agency (linked-agency.com) and BlueMoso (bluemoso.com). The operator (Joe Nilsen) discovered the fraud when his content was posted in an engagement pod without his knowledge. He then recorded a sales call where the pod operator (Cory Blumenfeld) admitted to running the operation.

## Key Entities

- **Linked Agency** — Content/growth agency. Co-founders: Cory Blumenfeld, Charlie Hills.
- **BlueMoso** (bluemoso.com) — VA company, 60+ team, Philippines-based operations. Supplies VAs who run engagement pods.
- **InfluencerMoso** — Brand partnerships arm. Email: hello@influencermoso.com.
- **Growth Community** — WhatsApp engagement pod. Created Feb 10, 2026. Operated by BlueMoso VAs.

## Project Structure

- data/raw/ — Original evidence (WhatsApp export, call transcript, MP3, emails, scraped profiles)
- data/parsed/ — Structured CSVs from parsed evidence
- data/scraped/ — LinkedIn data from Apify
- registry/ — Master tracking system (profiles, networks, VA-client map, evidence tags)
- analysis/ — Statistical outputs, engagement matrices, network graphs
- output/report/ — LinkedIn Trust & Safety formal report
- output/article/ — LinkedIn article drafts
- output/visualization/ — Interactive network visualization
- scripts/ — All Python scripts

## Registry System

profiles.csv is the single source of truth for every person in the investigation.

- Never add a profile outside the registry system.
- profile_id format: P001, P002, etc. Auto-increment.
- confidence tiers: confirmed (evidence-backed), probable (statistical anomaly), lead (flagged, no data yet)
- roles: operator, client, member, va, non-participant, unknown
- Use scripts/registry_manager.py for CLI management.

## Evidence Standard

Every claim in any published deliverable must be provable from:

1. Public LinkedIn data (comments, posts, engagement patterns), AND/OR
2. Private evidence (WhatsApp export, call recording, emails) offered to LinkedIn Trust & Safety
   No claim based on suspicion alone. If it can't survive a defamation challenge, it doesn't get published.

## Apify Scraper

- Actor: harvestapi~linkedin-profile-posts
- Supports: posts, comments, reactions (no session cookies needed)
- Token: stored in .env as APIFY_API_TOKEN
- Batch size: 5 URLs per run
- Default: 30 posts per profile, 100 comments per post, no reactions, no reposts

## Scripts

- scripts/parse_whatsapp.py — Parses raw WhatsApp export into structured CSVs
- scripts/registry_manager.py — CLI tool: add-profile, add-evidence, add-network, search, summary
- scripts/scrape_linkedin.py — Apify scraping pipeline with dry-run, resume, batch support
- scripts/build_engagement_matrix.py — Builds commenter-author matrix from scraped comments

## Rules

- All secrets in .env, never hardcoded
- Raw evidence files in data/raw/ are never modified
- Registry CSVs are the canonical data source — scripts read from them
- Every new evidence item gets logged in EVIDENCE-LOG.md
- When discovering new profiles through analysis, add them through the registry system with appropriate confidence tier
