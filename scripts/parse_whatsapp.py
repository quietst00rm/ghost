#!/usr/bin/env python3
"""Parse WhatsApp chat export into structured CSVs and auto-populate the registry."""

import csv
import re
import os
from collections import defaultdict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_CHAT = os.path.join(BASE_DIR, "data", "raw", "whatsapp-chat.txt")
PARSED_DIR = os.path.join(BASE_DIR, "data", "parsed")
REGISTRY_DIR = os.path.join(BASE_DIR, "registry")

# Regex for WhatsApp message lines: [M/D/YY, H:MM:SS AM/PM] sender: message
MSG_RE = re.compile(
    r'^\[(\d{1,2}/\d{1,2}/\d{2,4},\s*\d{1,2}:\d{2}:\d{2}\s*[AP]M)\]\s+'
    r'(.+?):\s(.*)'
)

LINKEDIN_POST_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/(?:posts/[^\s]+|feed/update/[^\s]+)',
    re.IGNORECASE
)

LINKEDIN_PROFILE_RE = re.compile(
    r'https?://(?:www\.)?linkedin\.com/in/([a-zA-Z0-9_-]+)',
    re.IGNORECASE
)

# Extract slug from post URL: /posts/USERNAME_...
# The slug is everything before the first _ in the path segment after /posts/
POST_AUTHOR_RE = re.compile(
    r'linkedin\.com/posts/([a-zA-Z][a-zA-Z0-9-]+?)_',
    re.IGNORECASE
)


def clean_sender(name):
    """Strip ~ prefix and trailing emoji from sender names."""
    name = name.strip()
    if name.startswith("~"):
        name = name[1:].strip()
    # Strip trailing emoji
    name = re.sub(r'[\s\U0001F300-\U0001FAD6\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F900-\U0001F9FF\U00002702-\U000027B0\U0000FE00-\U0000FE0F\u200d\u2640\u2642\u2600-\u26FF\u2700-\u27BF\U0000231A-\U0000231B♥️🔥🚀🤍]+$', '', name).strip()
    return name


def extract_slug_from_post_url(url):
    """Extract LinkedIn username slug from a post URL."""
    m = POST_AUTHOR_RE.search(url)
    if m:
        return m.group(1).lower()
    return None


def detect_va_client(sender_name, message_text):
    """
    Detect if this message is a VA posting for a client.
    Returns the client FIRST NAME (as used in the chat) or None.

    Patterns observed in the actual chat:
    - "Joe's post for today, please engage"
    - "Here is Liam's post for today!"
    - "Sharing Nick's post for today~"
    - "Sharing Kelly's post for today, please engage."
    - "Sharing Alan post for today :)"
    - "Sharing Emily Parcell post for today :)"
    - "Shawn's post is now live"
    - "Leo's post for today."
    - "Steven's post today"
    - "Frank's post today!"
    - "Ms. Cha's post today"
    - "All caught up for Cory!"
    - "All caught up for Steven"
    - "All caught up for Ms. Cha"
    - "All caught up for Kelly"
    - "All caught up for Emily"
    - "sharing with you Ms. Cha's post today"
    - "Louis' post today!"
    - "Karen's post today!"
    - "Ray's post is live now"
    - "Ben's post today!"
    - "Terry's post!!"
    - "Dr. Adeel's post for today"
    - "Sandy's post for today"
    - "Tiffany Masson's post today" (from Engagement Team Gex)
    - "Chris Murphy's post today." (from Engagement Team Gex)
    """
    text = message_text.strip()
    text_lower = text.lower()

    # Skip system messages
    if text.startswith("\u200e") or "joined using" in text_lower or "added" in text_lower and "post" not in text_lower:
        return None

    # Pattern 1: "[Name]'s post" - possessive
    # Matches: "Joe's post for today", "Shawn's post is now live", "Leo's post for today"
    # Also: "Ray's post is live now" (within longer sentence)
    m = re.search(
        r"(?:sharing(?:\s+with\s+you)?\s+)?(?:here\s+is\s+)?"
        r"([A-Z][a-zA-Z.']+(?:\s+[A-Z][a-zA-Z.']+){0,2}?)(?:'s|s')\s+post",
        text
    )
    # Also try to find possessive after a period/sentence break
    if not m:
        m = re.search(
            r"[.!]\s+([A-Z][a-zA-Z.']+(?:\s+[A-Z][a-zA-Z.']+){0,2}?)(?:'s|s')\s+post",
            text
        )
    if m:
        client = m.group(1).strip()
        # Filter self-references
        if client.lower() in ("my", "this", "the", "today", "here"):
            return None
        # Check it's not the sender referring to themselves
        if _name_matches(client, sender_name):
            return None
        return client

    # Pattern 2: "Sharing [Name] post for today" (no possessive, no "'s")
    # Matches: "Sharing Alan post for today :)", "Sharing Emily Parcell post for today :)"
    m = re.search(
        r"[Ss]haring\s+([A-Z][a-zA-Z.']+(?:\s+[A-Z][a-zA-Z.']+){0,2}?)\s+post",
        text
    )
    if m:
        client = m.group(1).strip()
        if client.lower() in ("my", "this", "the", "another"):
            return None
        if _name_matches(client, sender_name):
            return None
        return client

    # Pattern 3: "All caught up for [Name]"
    m = re.search(r"[Aa]ll\s+caught\s+up\s+for\s+([A-Z][a-zA-Z.']+(?:\s+[A-Z][a-zA-Z.']+){0,2})", text)
    if m:
        client = m.group(1).strip()
        # Strip trailing emoji/punctuation
        client = re.sub(r'[\s!🍕✍️🌶️:)]+$', '', client)
        if _name_matches(client, sender_name):
            return None
        return client

    # Pattern 4: Engagement Team style: "[Full Name]'s post today"
    m = re.search(r"([A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){1,2}?)(?:'s)?\s+post\s+today", text)
    if m:
        client = m.group(1).strip()
        if _name_matches(client, sender_name):
            return None
        return client

    return None


def _name_matches(client_name, sender_name):
    """Check if client name matches the sender (i.e. self-post, not VA)."""
    cl = client_name.lower().strip()
    sn = sender_name.lower().strip()
    if cl == sn:
        return True
    # Check if client first name is sender's first name
    if cl.split()[0] == sn.split()[0]:
        return True
    # Check containment
    if cl in sn or sn in cl:
        return True
    return False


# Known client name normalizations (map messy extractions to canonical names)
CLIENT_NORMALIZATIONS = {
    "Ms. Cha": "Cha Romero",
    "Dr. Adeel": "Dr. Adeel Khan",
    "Loui": "Louis",
    "Emily": "Emily Parcell",
    "Sharing Kelly": "Kelly",
    "Sharing Nick": "Nick",
    "Sharing Michael": "Michael",
    "Sharing Dr. Adeel": "Dr. Adeel Khan",
    "Sharing Sandy": "Sandy",
    "Happy Friday. Ray": "Ray",
    "Happy Monday. Ray": "Ray",
    "Happy Thursday. Ray": "Ray",
    "Happy Tuesday. Ray": "Ray",
    "Happy Wesnesday. Ray": "Ray",
}

# Known client name -> LinkedIn slug (from observing post URLs)
KNOWN_CLIENT_SLUGS = {
    "Joe": "joe-m-c",
    "Shreya": "shreyavohora",
    "Lucy": "lucyalligan",
    "John": "john-brewton-the-helper-strategy",
    "Nick": "realnickbradley",
    "Paul": "paulmatthewsai",
    "Kelly": "kellyamcginnis",
    "Liam": None,  # feed/update URLs only
    "Leo": "leonardrodman",
    "Shawn": "shawnfreeman-",
    "Karen": "karenstephen-ca",
    "Louis": "louisshulman",
    "Loui": "louisshulman",
    "Ben": "benpadnos",
    "Terry": "terry-zelen",
    "Michael": "michaeltatham",
    "Cha Romero": "charomero",
    "Ms. Cha": "charomero",
    "Steven": "stevenpettigrewinvestor",
    "Frank": "frank-ienzi",
    "Alan": "alan-brian-dardic",
    "Emily Parcell": "emilyparcell",
    "Dr. Adeel Khan": "dradeelkhan-md",
    "Sandy": "sandygrigsby",
    "Ray": "rayjbjang",
    "Cory": "coryblumenfeld",
}


def parse_messages():
    """Parse WhatsApp chat into list of message dicts."""
    messages = []
    with open(RAW_CHAT, "r", encoding="utf-8") as f:
        lines = f.readlines()

    current = None
    for line in lines:
        m = MSG_RE.match(line)
        if m:
            if current:
                messages.append(current)
            ts_str, sender, text = m.group(1), m.group(2), m.group(3)
            current = {
                "timestamp": ts_str,
                "sender_raw": sender,
                "sender_name": clean_sender(sender),
                "message_text": text.strip(),
            }
        elif current:
            current["message_text"] += "\n" + line.rstrip("\n")

    if current:
        messages.append(current)

    return messages


def main():
    messages = parse_messages()
    print(f"Total messages parsed: {len(messages)}")

    # Extract timestamps for date range
    dates = []
    for msg in messages:
        try:
            dt = datetime.strptime(msg["timestamp"], "%m/%d/%y, %I:%M:%S %p")
            dates.append(dt)
        except ValueError:
            pass
    if dates:
        print(f"Date range: {min(dates).strftime('%Y-%m-%d')} to {max(dates).strftime('%Y-%m-%d')}")

    # Unique senders (excluding system)
    senders = set()
    for msg in messages:
        name = msg["sender_name"]
        if name and name != "Growth Community":
            senders.add(name)
    print(f"Unique senders: {len(senders)}")

    # --- Write whatsapp-messages.csv ---
    msg_csv_path = os.path.join(PARSED_DIR, "whatsapp-messages.csv")
    with open(msg_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "sender_name", "message_text", "linkedin_urls"])
        for msg in messages:
            urls = LINKEDIN_POST_RE.findall(msg["message_text"])
            profile_urls = [f"https://www.linkedin.com/in/{s}" for s in LINKEDIN_PROFILE_RE.findall(msg["message_text"])]
            all_urls = urls + [u for u in profile_urls if u not in urls]
            seen = set()
            deduped = []
            for u in all_urls:
                clean = u.split("?")[0].rstrip("/")
                if clean not in seen:
                    seen.add(clean)
                    deduped.append(u)
            writer.writerow([
                msg["timestamp"],
                msg["sender_name"],
                msg["message_text"],
                "|".join(deduped) if deduped else ""
            ])
    print(f"Wrote {msg_csv_path}")

    # --- Extract pod posts and detect VA-client relationships ---
    post_rows = []
    all_post_urls = set()
    author_slugs = set()

    va_clients = defaultdict(set)       # va_name -> {client_name, ...}
    va_freq = defaultdict(lambda: defaultdict(int))  # va_name -> {client_name: count}
    va_evidence = defaultdict(list)     # va_name -> [(client, timestamp, snippet)]
    self_posters = set()
    sender_slug_map = {}                # sender_name -> linkedin_slug (for self-posters)

    for msg in messages:
        sender = msg["sender_name"]
        text = msg["message_text"]
        urls = LINKEDIN_POST_RE.findall(text)

        # Detect VA-client relationship from text (even without URL)
        client = detect_va_client(sender, text)
        if client:
            # Normalize client name
            client = CLIENT_NORMALIZATIONS.get(client, client)
            va_clients[sender].add(client)
            va_freq[sender][client] += 1
            va_evidence[sender].append((client, msg["timestamp"], text[:120]))

        if not urls:
            continue

        for url in urls:
            clean_url = url.split("?")[0].rstrip("/")
            slug = extract_slug_from_post_url(url)

            if clean_url not in all_post_urls:
                all_post_urls.add(clean_url)
                if slug:
                    author_slugs.add(slug)

            # Determine is_own_post
            is_own = False
            if client:
                # VA post for someone else
                is_own = False
            else:
                # Check if sender is the post author
                if slug:
                    sender_parts = sender.lower().replace(".", "").split()
                    slug_lower = slug.lower()
                    # Match if any substantial part of name is in slug
                    if any(part in slug_lower for part in sender_parts if len(part) > 2):
                        is_own = True
                        self_posters.add(sender)
                        sender_slug_map[sender] = slug

                if not is_own:
                    # Check for self-referential language
                    self_phrases = ["my post", "my latest", "grateful for your support", "hope your monday",
                                    "hope your", "i post daily", "my linkedin"]
                    if any(p in text.lower() for p in self_phrases):
                        is_own = True
                        self_posters.add(sender)
                        if slug:
                            sender_slug_map[sender] = slug

            if clean_url not in {r["post_url"] for r in post_rows}:
                post_rows.append({
                    "timestamp": msg["timestamp"],
                    "shared_by": sender,
                    "post_url": clean_url,
                    "post_author_slug": slug or "",
                    "is_own_post": str(is_own).lower(),
                })

    # Write pod-posts.csv
    posts_csv_path = os.path.join(PARSED_DIR, "pod-posts.csv")
    with open(posts_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "shared_by", "post_url", "post_author_slug", "is_own_post"])
        for row in post_rows:
            writer.writerow([row["timestamp"], row["shared_by"], row["post_url"],
                             row["post_author_slug"], row["is_own_post"]])
    print(f"Wrote {posts_csv_path}")
    print(f"\nUnique LinkedIn post URLs: {len(all_post_urls)}")
    print(f"Unique LinkedIn profile slugs (post authors): {len(author_slugs)}")

    # --- Classify roles ---
    va_names = {name for name, clients in va_clients.items() if clients}
    pure_vas = va_names - self_posters
    va_and_member = va_names & self_posters

    # Clients: people mentioned by VAs who never post in the group themselves
    all_clients_mentioned = set()
    for clients in va_clients.values():
        all_clients_mentioned.update(clients)

    posting_senders = {row["shared_by"] for row in post_rows}

    # Pure clients: mentioned by VAs but don't post themselves in the group
    # Need to check by matching first names to senders
    pure_clients = set()
    for client in all_clients_mentioned:
        # Check if this client name matches any sender
        is_sender = False
        for s in senders:
            if _name_matches(client, s):
                is_sender = True
                break
        if not is_sender:
            pure_clients.add(client)

    print(f"\n--- VAs (pure) identified: {len(pure_vas)} ---")
    for va in sorted(pure_vas):
        clients = sorted(va_clients[va])
        print(f"  {va}: posts for {', '.join(clients)}")

    print(f"\n--- VAs who also self-post: {len(va_and_member)} ---")
    for va in sorted(va_and_member):
        clients = sorted(va_clients[va])
        print(f"  {va}: posts for {', '.join(clients)}")

    pure_self = self_posters - va_names
    print(f"\n--- Self-posting members: {len(pure_self)} ---")
    for name in sorted(pure_self):
        print(f"  {name}")

    print(f"\n--- Clients (via VA posts, not in group): {len(pure_clients)} ---")
    for name in sorted(pure_clients):
        slug = KNOWN_CLIENT_SLUGS.get(name, "")
        print(f"  {name}" + (f" (linkedin.com/in/{slug})" if slug else ""))

    # --- Build Registry ---
    profiles = [
        {
            "profile_id": "P001", "linkedin_url": "https://www.linkedin.com/in/coryblumenfeld",
            "display_name": "Cory Blumenfeld", "whatsapp_name": "Cory Blumenfeld",
            "role": "operator", "confidence": "confirmed", "network": "linked-agency",
            "client_of": "",
            "notes": "Co-founder Linked Agency. Owns BlueMoso. Runs Growth Community pod. Confirmed on call.",
            "date_added": "2026-03-27",
        },
        {
            "profile_id": "P002", "linkedin_url": "https://www.linkedin.com/in/charlie-hills",
            "display_name": "Charlie Hills", "whatsapp_name": "",
            "role": "operator", "confidence": "confirmed", "network": "linked-agency",
            "client_of": "",
            "notes": "Co-founder Linked Agency. CC'd on post-call email.",
            "date_added": "2026-03-27",
        },
        {
            "profile_id": "P003", "linkedin_url": "https://www.linkedin.com/in/shanebarker",
            "display_name": "Shane Barker", "whatsapp_name": "",
            "role": "client", "confidence": "confirmed", "network": "linked-agency",
            "client_of": "",
            "notes": "Cory confirmed on call: provides VAs to Shane. Team runs his Twitter and LinkedIn.",
            "date_added": "2026-03-27",
        },
        {
            "profile_id": "P004", "linkedin_url": "https://www.linkedin.com/in/chasedimond",
            "display_name": "Chase Diamond", "whatsapp_name": "",
            "role": "unknown", "confidence": "lead", "network": "unknown",
            "client_of": "",
            "notes": "Suspected network connection. No data yet.",
            "date_added": "2026-03-27",
        },
        {
            "profile_id": "P005", "linkedin_url": "https://www.linkedin.com/in/joe-m-c",
            "display_name": "Joe Nilsen", "whatsapp_name": "",
            "role": "non-participant", "confidence": "confirmed", "network": "none",
            "client_of": "",
            "notes": "Content posted in pod without knowledge or consent. Never engaged.",
            "date_added": "2026-03-27",
        },
    ]

    next_id = 6
    known_urls = {p["linkedin_url"].lower().rstrip("/") for p in profiles}
    known_display = {p["display_name"].lower() for p in profiles}
    known_wa = {p["whatsapp_name"].lower() for p in profiles if p["whatsapp_name"]}

    def add_profile(display_name, whatsapp_name, role, li_slug, client_of_str, notes):
        nonlocal next_id
        li_url = f"https://www.linkedin.com/in/{li_slug}" if li_slug else ""
        if li_url and li_url.lower().rstrip("/") in known_urls:
            return
        if display_name.lower() in known_display:
            return
        if whatsapp_name and whatsapp_name.lower() in known_wa:
            return
        pid = f"P{next_id:03d}"
        next_id += 1
        profiles.append({
            "profile_id": pid, "linkedin_url": li_url,
            "display_name": display_name, "whatsapp_name": whatsapp_name,
            "role": role, "confidence": "confirmed", "network": "growth-community",
            "client_of": client_of_str, "notes": notes, "date_added": "2026-03-27",
        })
        if li_url:
            known_urls.add(li_url.lower().rstrip("/"))
        known_display.add(display_name.lower())
        if whatsapp_name:
            known_wa.add(whatsapp_name.lower())

    # Add VAs
    for va_name in sorted(va_names):
        slug = sender_slug_map.get(va_name, "")
        clients_list = sorted(va_clients[va_name])
        role = "va" if va_name in pure_vas else "va|member"
        client_of_str = "|".join(clients_list)
        add_profile(va_name, va_name, role, slug, client_of_str,
                     f"VA posting for {', '.join(clients_list)} in Growth Community.")

    # Add self-posting members (not VAs)
    for name in sorted(pure_self):
        slug = sender_slug_map.get(name, "")
        add_profile(name, name, "member", slug, "", "Self-posting member in Growth Community.")

    # Add pure clients
    for client_name in sorted(pure_clients):
        slug = KNOWN_CLIENT_SLUGS.get(client_name, "")
        # Find which VAs post for them
        vas_for = [va for va, clients in va_clients.items() if client_name in clients]
        add_profile(client_name, "", "client", slug, "",
                     f"Content posted by VA(s): {', '.join(vas_for)}.")

    # Add remaining senders who posted URLs but aren't yet categorized
    for name in sorted(posting_senders):
        if name == "Growth Community":
            continue
        slug = sender_slug_map.get(name, "")
        add_profile(name, name, "member", slug, "", "Active member in Growth Community.")

    # Write profiles.csv
    profiles_csv = os.path.join(REGISTRY_DIR, "profiles.csv")
    with open(profiles_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["profile_id", "linkedin_url", "display_name", "whatsapp_name",
                          "role", "confidence", "network", "client_of", "notes", "date_added"])
        for p in profiles:
            writer.writerow([p[k] for k in ["profile_id", "linkedin_url", "display_name",
                             "whatsapp_name", "role", "confidence", "network", "client_of",
                             "notes", "date_added"]])
    print(f"\nWrote {profiles_csv} ({len(profiles)} profiles)")

    # Write networks.csv
    networks_csv = os.path.join(REGISTRY_DIR, "networks.csv")
    with open(networks_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["network_id", "name", "platform", "operated_by", "created_date",
                          "discovered_date", "evidence_source", "member_count", "notes"])
        writer.writerow(["N001", "Growth Community", "WhatsApp", "BlueMoso/Linked Agency",
                          "2026-02-10", "2026-03-12", "whatsapp-chat.txt", len(senders),
                          "Primary pod. Cory confirmed ownership on call."])
    print(f"Wrote {networks_csv}")

    # Write va-client-map.csv
    va_map_csv = os.path.join(REGISTRY_DIR, "va-client-map.csv")
    with open(va_map_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["va_whatsapp_name", "va_role", "client_display_name",
                          "client_linkedin_slug", "frequency_in_export", "evidence"])
        for va_name in sorted(va_clients.keys()):
            for client_name in sorted(va_clients[va_name]):
                freq = va_freq[va_name][client_name]
                slug = KNOWN_CLIENT_SLUGS.get(client_name, "")
                evidence_list = [e for c, _, e in va_evidence[va_name] if c == client_name]
                evidence_str = evidence_list[0] if evidence_list else ""
                role = "va" if va_name in pure_vas else "va|member"
                writer.writerow([va_name, role, client_name, slug or "", freq, evidence_str])
    print(f"Wrote {va_map_csv}")

    # Write evidence-tags.csv
    evidence_csv = os.path.join(REGISTRY_DIR, "evidence-tags.csv")
    with open(evidence_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["profile_id", "evidence_type", "evidence_source", "timestamp", "description"])
        writer.writerow(["P001", "verbal_admission", "call-transcript.json", "21:57",
                          "Cory confirms Growth Community is his group"])
        writer.writerow(["P001", "verbal_admission", "call-transcript.json", "22:12",
                          "Cory states he runs multiple pods"])
        writer.writerow(["P001", "verbal_admission", "call-transcript.json", "22:46",
                          "Cory describes VA pod engagement as paid service"])
        writer.writerow(["P001", "verbal_admission", "call-transcript.json", "23:20",
                          "Cory names GoLogin anti-detect browser"])
        writer.writerow(["P001", "verbal_admission", "call-transcript.json", "27:00",
                          "Cory confirms BlueMoso is his company"])
        writer.writerow(["P003", "verbal_admission", "call-transcript.json", "02:28",
                          "Cory says he provides Shane with VAs"])
        writer.writerow(["P003", "verbal_admission", "call-transcript.json", "03:11",
                          "Cory says team runs Shane's Twitter and LinkedIn"])
    print(f"Wrote {evidence_csv}")

    # --- FINAL SUMMARY ---
    print("\n" + "=" * 60)
    print("FINAL SUMMARY")
    print("=" * 60)
    print(f"Total WhatsApp messages parsed: {len(messages)}")
    if dates:
        print(f"Date range: {min(dates).strftime('%Y-%m-%d')} to {max(dates).strftime('%Y-%m-%d')}")
    print(f"Unique senders in chat: {len(senders)}")
    print(f"Unique LinkedIn post URLs shared: {len(all_post_urls)}")
    print(f"Unique LinkedIn profiles identified (post authors): {len(author_slugs)}")

    print(f"\nVAs identified: {len(va_names)}")
    for va in sorted(va_names):
        clients = sorted(va_clients[va])
        label = " (also self-posts)" if va in self_posters else ""
        print(f"  - {va}{label}: posts for {', '.join(clients)}")

    print(f"\nSelf-posting members (not VAs): {len(pure_self)}")
    for name in sorted(pure_self):
        print(f"  - {name}")

    print(f"\nClients identified via VA posts: {len(pure_clients)}")
    for name in sorted(pure_clients):
        print(f"  - {name}")

    print(f"\nTotal profiles in registry: {len(profiles)}")


if __name__ == "__main__":
    main()
