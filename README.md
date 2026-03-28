# Ghost Sweep

Investigation into coordinated LinkedIn engagement fraud operated by Linked Agency (linked-agency.com) and BlueMoso (bluemoso.com).

## Co-founders

- **Cory Blumenfeld** - Co-founder, Linked Agency / BlueMoso
- **Charlie Hills** - Co-founder, Linked Agency

## Purpose

Document and prove coordinated LinkedIn engagement pod activity, including:
- WhatsApp-based engagement pods where VAs and members exchange likes, saves, and comments
- Virtual assistant (VA) networks posting on behalf of clients
- Anti-detect browser usage (GoLogin) to operate multiple LinkedIn accounts
- Systematic manipulation of LinkedIn's algorithm through artificial engagement

## Key Rule

Every published claim must be provable from public LinkedIn data and/or private evidence (WhatsApp export, call recording).

## Directory Structure

- `data/raw/` - Original evidence files (WhatsApp export, call transcript, audio). Gitignored.
- `data/parsed/` - Structured CSVs derived from raw evidence.
- `data/scraped/` - LinkedIn data pulled via Apify. Gitignored.
- `registry/` - Master tracking files: profiles, networks, VA-client mappings, evidence tags.
- `analysis/` - Intermediate analysis outputs.
- `output/report/` - Final investigation report.
- `output/article/` - Published article drafts.
- `output/visualization/` - Charts, graphs, network diagrams.
- `scripts/` - Utility scripts (registry manager, scrapers, parsers).

## Registry System

`registry/profiles.csv` is the master record. Every person gets a confidence tier:
- **confirmed** - Direct evidence from call transcript, WhatsApp export, or public LinkedIn data
- **probable** - Strong circumstantial evidence (pattern matching, timing analysis)
- **lead** - Suspected connection, needs further investigation

New profiles are ONLY added through the registry system (`scripts/registry_manager.py`).

## Apify Integration

Actor: `harvestapi~linkedin-profile-posts` (supports posts, comments, reactions).
Token stored in `.env`.
