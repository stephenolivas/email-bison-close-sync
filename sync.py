"""
EmailBison → Close CRM Sync
Polls EmailBison for replies across configured campaigns and creates/updates
leads + contacts in Close CRM. Tracks processed reply IDs in state.json to
avoid duplicates across runs.

Fixes applied:
  1. Skips automated/OOO replies — only syncs genuine human replies
  2. Searches for existing company leads first; adds as contact if found
  3. Contact name is correctly populated on create and update
  4. All new leads are assigned to Daniel Bustos via Lead Owner custom field
"""

import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

EMAILBISON_API_KEY = os.environ["EMAILBISON_API_KEY"]
EMAILBISON_BASE_URL = os.environ["EMAILBISON_BASE_URL"].rstrip("/")

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
CLOSE_BASE_URL = "https://api.close.com/api/v1"

# Daniel Bustos — assigned to all new/updated leads
DANIEL_MEMBER_ID = "memb_uwjFLJqk0bD1usjzeAMZIZq3CvENPYTZUbpctAXtCQ8"

# Custom field key for Lead Owner — find this in your existing Close scripts
# or go to Settings → Custom Fields in Close and check the API name.
# It will look like: "custom.cf_xxxxxxxxxxxxxxxxxxxxxxxx"
LEAD_OWNER_FIELD = os.environ.get("CLOSE_LEAD_OWNER_FIELD", "custom.Lead Owner")

TARGET_CAMPAIGNS = [
    "Irving Campaign March 23rd 2026",
    "David Campaign March 23rd 2026",
    "Barry Campaign March 23rd 2026",
]

# EmailBison reply categories/types that indicate automated/OOO — not real humans
AUTO_REPLY_INDICATORS = {
    "auto_reply",
    "auto-reply",
    "automated_reply",
    "automated reply",
    "out_of_office",
    "out-of-office",
    "ooo",
    "bounce",
    "bounced",
    "unsubscribe",
}

STATE_FILE = Path(__file__).parent / "state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"processed_reply_ids": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# EmailBison helpers
# ---------------------------------------------------------------------------

EB_HEADERS = {
    "Authorization": f"Bearer {EMAILBISON_API_KEY}",
    "Content-Type": "application/json",
}


def eb_get(path: str, params: dict = None) -> dict:
    url = f"{EMAILBISON_BASE_URL}/api{path}"
    for attempt in range(3):
        resp = requests.get(url, headers=EB_HEADERS, params=params, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", resp.json().get("retry_after", 10)))
            log.warning("EmailBison rate limited. Waiting %ss...", retry_after)
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("EmailBison: exceeded retry limit on rate limiting.")


def is_automated_reply(reply: dict) -> bool:
    """
    Return True if this reply looks like an auto-reply / OOO / bounce.
    Checks type/category fields, tags array, and subject line heuristics.
    """
    for field in ("type", "category", "reply_type", "status"):
        val = str(reply.get(field, "")).lower().replace(" ", "_")
        if val in AUTO_REPLY_INDICATORS:
            log.info("   Skipping automated reply (field '%s' = '%s')", field, val)
            return True

    tags = reply.get("tags") or []
    if isinstance(tags, list):
        for tag in tags:
            if str(tag).lower().replace(" ", "_") in AUTO_REPLY_INDICATORS:
                log.info("   Skipping automated reply (tag = '%s')", tag)
                return True

    subject = str(reply.get("subject") or reply.get("email_subject") or "").lower()
    auto_subject_keywords = (
        "automatic reply", "auto-reply", "out of office", "automatische antwort"
    )
    for kw in auto_subject_keywords:
        if kw in subject:
            log.info("   Skipping automated reply (subject contains '%s')", kw)
            return True

    return False


def get_target_campaign_ids() -> dict:
    data = eb_get("/campaigns")
    campaigns = data.get("data", data) if isinstance(data, dict) else data
    found = {}
    for c in campaigns:
        if c.get("name") in TARGET_CAMPAIGNS:
            found[c["name"]] = c["id"]
    missing = set(TARGET_CAMPAIGNS) - set(found.keys())
    if missing:
        log.warning("Could not find EmailBison campaign(s): %s", missing)
    return found


def get_replies_for_campaign(campaign_id: int) -> list:
    data = eb_get("/replies", params={"campaign_id": campaign_id})
    return data.get("data", data) if isinstance(data, dict) else data


def get_lead_by_id(lead_id: int) -> dict:
    data = eb_get(f"/leads/{lead_id}")
    return data.get("data", data) if isinstance(data, dict) else data


def extract_contact_fields(eb_lead: dict) -> dict:
    first = eb_lead.get("first_name", "")
    last  = eb_lead.get("last_name", "")
    full  = eb_lead.get("full_name") or eb_lead.get("name", "")
    if not full and (first or last):
        full = f"{first} {last}".strip()
    return {
        "full_name": full.strip(),
        "email":     eb_lead.get("email", "").strip().lower(),
        "job_title": eb_lead.get("title") or eb_lead.get("job_title", ""),
        "phone":     eb_lead.get("phone") or eb_lead.get("phone_number", ""),
        "company":   eb_lead.get("company") or eb_lead.get("company_name", ""),
        "website":   eb_lead.get("website") or eb_lead.get("url", ""),
        "address":   eb_lead.get("address") or eb_lead.get("address_1", ""),
        "city":      eb_lead.get("city", ""),
        "state":     eb_lead.get("state", ""),
        "zipcode":   eb_lead.get("zipcode") or eb_lead.get("postal_code", ""),
        "country":   eb_lead.get("country", ""),
    }


# ---------------------------------------------------------------------------
# Close CRM helpers
# ---------------------------------------------------------------------------

CLOSE_AUTH    = (CLOSE_API_KEY, "")
CLOSE_HEADERS = {"Content-Type": "application/json"}


def close_get(path: str, params: dict = None) -> dict:
    resp = requests.get(
        f"{CLOSE_BASE_URL}{path}", auth=CLOSE_AUTH,
        headers=CLOSE_HEADERS, params=params, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def close_post(path: str, payload: dict) -> dict:
    resp = requests.post(
        f"{CLOSE_BASE_URL}{path}", auth=CLOSE_AUTH,
        headers=CLOSE_HEADERS, json=payload, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def close_put(path: str, payload: dict) -> dict:
    resp = requests.put(
        f"{CLOSE_BASE_URL}{path}", auth=CLOSE_AUTH,
        headers=CLOSE_HEADERS, json=payload, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def find_lead_by_email(email: str) -> dict | None:
    result = close_get("/lead/", params={"query": f'email_address:"{email}"'})
    leads = result.get("data", [])
    return leads[0] if leads else None


def find_lead_by_company(company_name: str) -> dict | None:
    if not company_name:
        return None
    result = close_get("/lead/", params={"query": f'name:"{company_name}"'})
    leads = result.get("data", [])
    return leads[0] if leads else None


def find_contact_by_email(lead: dict, email: str) -> dict | None:
    for contact in lead.get("contacts", []):
        for e in contact.get("emails", []):
            if e.get("email", "").lower() == email.lower():
                return contact
    return None


# ---------------------------------------------------------------------------
# Create / update logic
# ---------------------------------------------------------------------------

def lead_owner_payload() -> dict:
    return {LEAD_OWNER_FIELD: DANIEL_MEMBER_ID}


def build_new_contact(fields: dict) -> dict:
    contact: dict = {}
    if fields["full_name"]:
        contact["name"] = fields["full_name"]
    if fields["job_title"]:
        contact["title"] = fields["job_title"]
    if fields["email"]:
        contact["emails"] = [{"type": "office", "email": fields["email"]}]
    if fields["phone"]:
        contact["phones"] = [{"type": "office", "phone": fields["phone"]}]
    return contact


def create_brand_new_lead(fields: dict) -> dict:
    """Create a new lead + embedded contact in Close, assigned to Daniel."""
    payload: dict = {**lead_owner_payload()}
    payload["name"] = fields["company"] or fields["full_name"] or "Unknown"

    if fields["website"]:
        payload["url"] = fields["website"]

    if any(fields[k] for k in ("address", "city", "state", "zipcode", "country")):
        payload["addresses"] = [{
            "label":     "office",
            "address_1": fields["address"],
            "city":      fields["city"],
            "state":     fields["state"],
            "zipcode":   fields["zipcode"],
            "country":   fields["country"],
        }]

    contact = build_new_contact(fields)
    if contact:
        payload["contacts"] = [contact]

    lead = close_post("/lead/", payload)
    log.info("  Created new lead '%s' (id=%s)", lead.get("name"), lead.get("id"))
    return lead


def add_contact_to_existing_lead(lead: dict, fields: dict) -> None:
    """Add a new contact to an existing lead and assign to Daniel."""
    lead_id = lead["id"]
    contact = build_new_contact(fields)
    if contact:
        contact["lead_id"] = lead_id
        close_post("/contact/", contact)
        log.info(
            "  Added contact '%s' to existing lead '%s'",
            fields["full_name"] or fields["email"], lead.get("name")
        )
    if not lead.get(LEAD_OWNER_FIELD):
        close_put(f"/lead/{lead_id}/", lead_owner_payload())
        log.info("  Assigned lead to Daniel Bustos")


def update_existing_contact(lead: dict, contact: dict, fields: dict) -> None:
    """Patch empty fields on existing lead + contact, assign to Daniel."""
    lead_id    = lead["id"]
    contact_id = contact["id"]

    lead_updates: dict = {**lead_owner_payload()}
    if fields["website"] and not lead.get("url"):
        lead_updates["url"] = fields["website"]
    if fields["company"] and not lead.get("name"):
        lead_updates["name"] = fields["company"]
    if fields["address"] and not lead.get("addresses"):
        lead_updates["addresses"] = [{
            "label":     "office",
            "address_1": fields["address"],
            "city":      fields["city"],
            "state":     fields["state"],
            "zipcode":   fields["zipcode"],
            "country":   fields["country"],
        }]
    close_put(f"/lead/{lead_id}/", lead_updates)

    contact_updates: dict = {}
    if fields["full_name"] and not contact.get("name"):
        contact_updates["name"] = fields["full_name"]
    if fields["job_title"] and not contact.get("title"):
        contact_updates["title"] = fields["job_title"]
    if fields["phone"] and not contact.get("phones"):
        contact_updates["phones"] = [{"type": "office", "phone": fields["phone"]}]

    if contact_updates:
        close_put(f"/contact/{contact_id}/", contact_updates)
        log.info("  Updated contact fields: %s", list(contact_updates.keys()))

    log.info("  Updated lead '%s' — assigned to Daniel", lead.get("name"))


# ---------------------------------------------------------------------------
# Main sync loop
# ---------------------------------------------------------------------------

def run_sync() -> None:
    state = load_state()
    processed_ids: set = set(state.get("processed_reply_ids", []))

    campaign_ids = get_target_campaign_ids()
    if not campaign_ids:
        log.error("No matching campaigns found — check TARGET_CAMPAIGNS names.")
        sys.exit(1)

    new_processed: list = []
    total_created = 0
    total_updated = 0
    total_skipped = 0
    total_auto    = 0

    for campaign_name, campaign_id in campaign_ids.items():
        log.info("Processing campaign: %s (id=%s)", campaign_name, campaign_id)
        replies = get_replies_for_campaign(campaign_id)
        log.info("  Found %d replies.", len(replies))

        for reply in replies:
            reply_id = str(reply.get("id"))

            if reply_id in processed_ids:
                total_skipped += 1
                continue

            if is_automated_reply(reply):
                total_auto += 1
                new_processed.append(reply_id)
                continue

            lead_id = reply.get("lead_id") or reply.get("leadId")
            if not lead_id:
                log.warning("  Reply %s has no lead_id — skipping.", reply_id)
                new_processed.append(reply_id)
                continue

            try:
                eb_lead = get_lead_by_id(lead_id)
            except Exception as exc:
                log.error("  Failed to fetch EmailBison lead %s: %s", lead_id, exc)
                continue

            fields = extract_contact_fields(eb_lead)

            if not fields["email"]:
                log.warning("  Lead %s has no email — skipping.", lead_id)
                new_processed.append(reply_id)
                continue

            log.info(
                "  -> %s <%s> @ %s",
                fields["full_name"] or "(no name)",
                fields["email"],
                fields["company"] or "(no company)",
            )

            try:
                # Priority 1: match by email
                existing_lead = find_lead_by_email(fields["email"])
                if existing_lead:
                    contact = find_contact_by_email(existing_lead, fields["email"])
                    if contact:
                        log.info("  Email matched existing lead — updating.")
                        update_existing_contact(existing_lead, contact, fields)
                    else:
                        log.info("  Lead matched by email — adding contact.")
                        add_contact_to_existing_lead(existing_lead, fields)
                    total_updated += 1

                else:
                    # Priority 2: match by company name
                    company_lead = find_lead_by_company(fields["company"])
                    if company_lead:
                        log.info(
                            "  Company '%s' already exists — adding contact.",
                            fields["company"]
                        )
                        add_contact_to_existing_lead(company_lead, fields)
                        total_updated += 1
                    else:
                        # Priority 3: create new lead
                        log.info("  No match found — creating new lead.")
                        create_brand_new_lead(fields)
                        total_created += 1

                new_processed.append(reply_id)

            except requests.HTTPError as exc:
                log.error("  Close API error for reply %s: %s", reply_id, exc)
                continue

    state["processed_reply_ids"] = list(processed_ids | set(new_processed))
    save_state(state)

    log.info(
        "Sync complete — created: %d | updated: %d | auto-replies filtered: %d | already synced: %d",
        total_created, total_updated, total_auto, total_skipped,
    )


if __name__ == "__main__":
    run_sync()
