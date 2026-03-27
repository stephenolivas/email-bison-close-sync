"""
EmailBison → Close CRM Sync
Polls EmailBison for replies across configured campaigns and creates/updates
leads + contacts in Close CRM. Tracks processed reply IDs in state.json to
avoid duplicates across runs.
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
EMAILBISON_BASE_URL = os.environ["EMAILBISON_BASE_URL"].rstrip("/")  # e.g. https://dedi.emailbison.com

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
CLOSE_BASE_URL = "https://api.close.com/api/v1"

# Campaigns to watch — must match names exactly as they appear in EmailBison
TARGET_CAMPAIGNS = [
    "Irving Campaign March 23rd 2026",
    "David Campaign March 23rd 2026",
    "Barry Campaign March 23rd 2026",
]

STATE_FILE = Path(__file__).parent / "state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State helpers (tracks which reply IDs have already been synced)
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"processed_reply_ids": []}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# EmailBison API helpers
# ---------------------------------------------------------------------------

EB_HEADERS = {
    "Authorization": f"Bearer {EMAILBISON_API_KEY}",
    "Content-Type": "application/json",
}


def eb_get(path: str, params: dict = None) -> dict:
    """GET from EmailBison with basic rate-limit handling."""
    url = f"{EMAILBISON_BASE_URL}/api{path}"
    for attempt in range(3):
        resp = requests.get(url, headers=EB_HEADERS, params=params, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", resp.json().get("retry_after", 10)))
            log.warning("EmailBison rate limited. Waiting %ss…", retry_after)
            time.sleep(retry_after)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError("EmailBison: exceeded retry limit on rate limiting.")


def get_target_campaign_ids() -> dict[str, int]:
    """Return {campaign_name: campaign_id} for each campaign in TARGET_CAMPAIGNS."""
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


def get_replies_for_campaign(campaign_id: int) -> list[dict]:
    """Fetch all replies for a given campaign."""
    data = eb_get("/replies", params={"campaign_id": campaign_id})
    return data.get("data", data) if isinstance(data, dict) else data


def get_lead_by_id(lead_id: int) -> dict:
    """Fetch a single EmailBison lead record."""
    data = eb_get(f"/leads/{lead_id}")
    return data.get("data", data) if isinstance(data, dict) else data


def extract_contact_fields(eb_lead: dict) -> dict:
    """
    Normalise an EmailBison lead object into the fields we care about.
    EmailBison stores contact info at the lead level; adjust key names
    here if your workspace uses different field names.
    """
    return {
        "full_name":   eb_lead.get("full_name") or eb_lead.get("name", "").strip(),
        "email":       eb_lead.get("email", "").strip().lower(),
        "job_title":   eb_lead.get("title") or eb_lead.get("job_title", ""),
        "phone":       eb_lead.get("phone") or eb_lead.get("phone_number", ""),
        "company":     eb_lead.get("company") or eb_lead.get("company_name", ""),
        "website":     eb_lead.get("website") or eb_lead.get("url", ""),
        "address":     eb_lead.get("address") or eb_lead.get("address_1", ""),
        "city":        eb_lead.get("city", ""),
        "state":       eb_lead.get("state", ""),
        "zipcode":     eb_lead.get("zipcode") or eb_lead.get("postal_code", ""),
        "country":     eb_lead.get("country", ""),
    }


# ---------------------------------------------------------------------------
# Close CRM API helpers
# ---------------------------------------------------------------------------

CLOSE_AUTH = (CLOSE_API_KEY, "")  # Close uses API key as Basic auth username
CLOSE_HEADERS = {"Content-Type": "application/json"}


def close_get(path: str, params: dict = None) -> dict:
    url = f"{CLOSE_BASE_URL}{path}"
    resp = requests.get(url, auth=CLOSE_AUTH, headers=CLOSE_HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def close_post(path: str, payload: dict) -> dict:
    url = f"{CLOSE_BASE_URL}{path}"
    resp = requests.post(url, auth=CLOSE_AUTH, headers=CLOSE_HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def close_put(path: str, payload: dict) -> dict:
    url = f"{CLOSE_BASE_URL}{path}"
    resp = requests.put(url, auth=CLOSE_AUTH, headers=CLOSE_HEADERS, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def find_lead_by_email(email: str) -> dict | None:
    """Search Close for any lead that has this email on a contact."""
    result = close_get("/lead/", params={"query": f'email_address:"{email}"'})
    leads = result.get("data", [])
    return leads[0] if leads else None


def find_contact_by_email(lead: dict, email: str) -> dict | None:
    """Find the specific contact on a lead that matches the email."""
    for contact in lead.get("contacts", []):
        for e in contact.get("emails", []):
            if e.get("email", "").lower() == email.lower():
                return contact
    return None


# ---------------------------------------------------------------------------
# Core create / update logic
# ---------------------------------------------------------------------------

def build_lead_payload(fields: dict) -> dict:
    """Build a Close lead creation payload from normalised fields."""
    payload: dict = {}

    if fields["company"]:
        payload["name"] = fields["company"]
    elif fields["full_name"]:
        payload["name"] = fields["full_name"]  # fallback for B2C

    if fields["website"]:
        payload["url"] = fields["website"]

    # Address
    if any(fields[k] for k in ("address", "city", "state", "zipcode", "country")):
        payload["addresses"] = [{
            "label":      "office",
            "address_1":  fields["address"],
            "city":       fields["city"],
            "state":      fields["state"],
            "zipcode":    fields["zipcode"],
            "country":    fields["country"],
        }]

    # Embed contact inline on creation
    contact: dict = {}
    if fields["full_name"]:
        contact["name"] = fields["full_name"]
    if fields["job_title"]:
        contact["title"] = fields["job_title"]
    if fields["email"]:
        contact["emails"] = [{"type": "office", "email": fields["email"]}]
    if fields["phone"]:
        contact["phones"] = [{"type": "office", "phone": fields["phone"]}]

    if contact:
        payload["contacts"] = [contact]

    return payload


def create_lead(fields: dict) -> dict:
    payload = build_lead_payload(fields)
    lead = close_post("/lead/", payload)
    log.info("  ✅ Created Close lead '%s' (id=%s)", lead.get("name"), lead.get("id"))
    return lead


def update_existing_lead(lead: dict, contact: dict, fields: dict) -> None:
    """Patch lead-level fields and the matched contact."""
    lead_id = lead["id"]
    lead_updates: dict = {}

    # Only update lead-level fields if they're currently empty
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

    if lead_updates:
        close_put(f"/lead/{lead_id}/", lead_updates)
        log.info("  ✏️  Updated lead fields: %s", list(lead_updates.keys()))

    # Update contact
    contact_id = contact["id"]
    contact_updates: dict = {}

    if fields["full_name"] and not contact.get("name"):
        contact_updates["name"] = fields["full_name"]
    if fields["job_title"] and not contact.get("title"):
        contact_updates["title"] = fields["job_title"]
    if fields["phone"] and not contact.get("phones"):
        contact_updates["phones"] = [{"type": "office", "phone": fields["phone"]}]

    if contact_updates:
        close_put(f"/contact/{contact_id}/", contact_updates)
        log.info("  ✏️  Updated contact fields: %s", list(contact_updates.keys()))

    if not lead_updates and not contact_updates:
        log.info("  ↩️  Lead/contact already up to date, no changes needed.")


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

    for campaign_name, campaign_id in campaign_ids.items():
        log.info("📋 Processing campaign: %s (id=%s)", campaign_name, campaign_id)
        replies = get_replies_for_campaign(campaign_id)
        log.info("   Found %d replies.", len(replies))

        for reply in replies:
            reply_id = str(reply.get("id"))

            if reply_id in processed_ids:
                total_skipped += 1
                continue

            lead_id = reply.get("lead_id") or reply.get("leadId")
            if not lead_id:
                log.warning("   Reply %s has no lead_id — skipping.", reply_id)
                new_processed.append(reply_id)
                continue

            # Fetch full lead data from EmailBison
            try:
                eb_lead = get_lead_by_id(lead_id)
            except Exception as exc:
                log.error("   Failed to fetch EmailBison lead %s: %s", lead_id, exc)
                continue

            fields = extract_contact_fields(eb_lead)

            if not fields["email"]:
                log.warning("   Lead %s has no email — skipping.", lead_id)
                new_processed.append(reply_id)
                continue

            log.info("   → %s <%s> @ %s", fields["full_name"], fields["email"], fields["company"])

            try:
                existing_lead = find_lead_by_email(fields["email"])

                if existing_lead:
                    contact = find_contact_by_email(existing_lead, fields["email"])
                    if contact:
                        log.info("   🔍 Lead exists in Close — updating.")
                        update_existing_lead(existing_lead, contact, fields)
                        total_updated += 1
                    else:
                        log.warning("   Lead found but contact not matched — skipping update.")
                else:
                    log.info("   🆕 New lead — creating in Close.")
                    create_lead(fields)
                    total_created += 1

                new_processed.append(reply_id)

            except requests.HTTPError as exc:
                log.error("   Close API error for reply %s: %s", reply_id, exc)
                # Don't mark as processed so it retries next run
                continue

    # Persist updated state
    state["processed_reply_ids"] = list(processed_ids | set(new_processed))
    save_state(state)

    log.info(
        "✅ Sync complete — created: %d | updated: %d | skipped (already synced): %d",
        total_created, total_updated, total_skipped,
    )


if __name__ == "__main__":
    run_sync()
