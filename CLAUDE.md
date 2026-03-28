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
- scripts/scrape_expansion.py — Scrapes second-ring expansion targets via Apify
- scripts/build_engagement_matrix.py — Builds commenter-author matrix from scraped comments
- scripts/build_reciprocal_pairs.py — Identifies reciprocal engagement pairs (both directions >= 3)
- scripts/detect_clusters.py — Louvain community detection, bridge accounts, third-ring candidates
- scripts/full_commenter_census.py — Full census of all 9,731 commenters across 60K+ comments
- scripts/timing_analysis.py — Response time and burst pattern detection for coordinated commenting
- scripts/comment_text_analysis.py — Generic phrase detection and VA behavior flagging
- scripts/full_network_map.py — Expanded network graph with cluster detection (5+ author threshold)
- scripts/master_target_list.py — Prioritized scrape target list combining all analysis signals
- scripts/game_plan_network.py — Maps The Game Plan's 68 targets and co-commenters
- scripts/deep_profile_analysis.py — Deep commenter analysis for Victor Trieu, Charlie Hills, Chris Lang
- scripts/identify_vas.py — Multi-signal VA identification (generic%, response time, bursts, operator overlap)
- scripts/operator_footprint.py — Cory/Charlie outgoing/incoming comment mapping and shared targets
- scripts/ring_tiers.py — Evidence-based tier assignment (1-6) and mass registry update (--register flag)
- scripts/build_public_viz_data.py — Builds public visualization data.json from analysis files (anonymized names, no private evidence)

## Analysis Outputs

- data/analysis/engagement-matrix.csv — 72x72 commenter-vs-author matrix
- data/analysis/top-pairs.csv — All 1,232 directed pairs sorted by count
- data/analysis/reciprocal-pairs.csv — 345 reciprocal pairs (both directions >= 3)
- data/analysis/external-commenters.csv — 8,198 non-registry commenters on registry posts
- data/analysis/clusters.csv — Per-member cluster assignments (4 clusters, 49 nodes)
- data/analysis/cluster-summary.csv — Per-cluster stats (density, hub, WhatsApp overlap)
- data/analysis/bridge-accounts.csv — 40 accounts connecting 2+ clusters
- data/analysis/third-ring-candidates.csv — 69 accounts for next scrape wave
- data/analysis/statistical-summary.md — Full statistical report with organic comparisons
- data/analysis/shane-top-commenters.csv — 165 commenters on Shane Barker's 30 scraped posts
- data/analysis/shane-scrape-queue.csv — 35 unsscraped Shane commenters (5+ comments) for next wave
- data/parsed/shane-barker-posts.csv — 525 Shane Barker posts (Apr 2025 - Mar 2026), 22,418 total comments
- data/analysis/full-commenter-census.csv — 9,731 unique commenters, 145 hitting 10+ authors (91 unregistered)
- data/analysis/comment-timing.csv — Response time stats for 145 high-frequency commenters
- data/analysis/burst-events.csv — 1,453 burst events (3+ ring commenters within 60min)
- data/analysis/comment-quality.csv — Text quality scores for 835 commenters, 39 suspected VA accounts
- data/analysis/full-network-pairs.csv — 4,387 directed engagement pairs (5+ author threshold)
- data/analysis/full-network-clusters.csv — 386 accounts in expanded network (311 new)
- data/analysis/master-ring-members.csv — 376 prioritized targets (17 critical, 70 high)
- data/analysis/game-plan-targets.csv — 68 profiles targeted by The Game Plan bot account
- data/analysis/game-plan-co-commenters.csv — Co-commenters shadowing The Game Plan across targets
- data/analysis/victor-trieu-commenters.csv — 617 commenters on Victor Trieu's posts
- data/analysis/charlie-hills-commenters.csv — 1,453 commenters on Charlie Hills' posts
- data/analysis/chris-lang-commenters.csv — 724 commenters on Chris Lang's posts
- data/analysis/va-identification.csv — 78 accounts flagged with VA behavioral signals (score 1-5)
- data/analysis/operator-footprint.csv — Cory/Charlie incoming/outgoing engagement map
- data/analysis/operator-shared-targets.csv — Profiles both operators engage with
- data/analysis/ring-tiers.csv — 418 accounts with evidence-based tier assignments (1-6)

## Networks

- **growth-community** — Original WhatsApp pod (P001-P068)
- **growth-community-extended** — Second ring discovered via reciprocal pairs (P069-P092)
- **barker-network** — Shane Barker's high-frequency commenters (P093-P126), 9 probable + 25 leads
- **extended-network** / **cluster-N** — Tier 3-4 accounts discovered via data analysis (P128-P185)

## Ring Tiers

- Tier 1: Confirmed Operator (2) - direct evidence
- Tier 2: Confirmed Participant (53) - WhatsApp/VA-client proven
- Tier 3: Proven by Data (68) - reciprocal 5+ confirmed, 10+ bursts, or VA score 3+
- Tier 4: Strong Indicator (3) - 20+ ring targets, reciprocal 3-4 confirmed
- Tier 5: Suspected (34) - not registered
- Tier 6: Lead (258) - not registered
- Total registered: 161 profiles (P001-P185)

## Public Visualization

- output/visualization/index.html — Single-file 5-view interactive platform (D3.js)
- output/visualization/data.json — 160 Tier 1-5 nodes, 1,542 edges, anonymized names
- Views: Network Map, Ring Structure, Burst Timeline, Automated Accounts, Evidence
- Names displayed as "First L." format (e.g., "Cory B.")
- NO private evidence references (WhatsApp, call recording, emails, VAs, company names)
- All claims provable from public LinkedIn data alone
- Rebuild data.json: python3 scripts/build_public_viz_data.py

## Rules

- All secrets in .env, never hardcoded
- Raw evidence files in data/raw/ are never modified
- Registry CSVs are the canonical data source — scripts read from them
- Every new evidence item gets logged in EVIDENCE-LOG.md
- When discovering new profiles through analysis, add them through the registry system with appropriate confidence tier
