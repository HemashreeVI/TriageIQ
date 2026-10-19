"""
TriageIQ - turn a raw email into a ticket.

Pure functions (no network), so they are easy to test. The important job here
is SAFETY: never create tickets from automatic mail, bounces, newsletters or
our own messages, otherwise two auto-responders can email each other forever.
"""

import hashlib
import html
import re
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
from email.utils import parseaddr

NO_REPLY = re.compile(r"(no-?reply|do-?not-?reply|mailer-daemon|postmaster|bounce)", re.I)
QUOTE_START = re.compile(r"^(On .{5,120} wrote:|-{2,}\s*Original Message\s*-{2,}|_{5,}|From: .+)\s*$", re.I)
MAX_BODY_CHARS = 3000
MAX_SUBJECT_CHARS = 200


def clean_line(text):
    """One line, no control characters (stops header tricks later when we reply)."""
    return re.sub(r"[\x00-\x1f\x7f]+", " ", text or "").strip()


def parse_email(raw: bytes):
    return BytesParser(policy=policy.default).parsebytes(raw)


def skip_reason(msg, support_email):
    """Why this message must NOT become a ticket, or None if it is fine."""
    sender = parseaddr(msg.get("From", ""))[1].lower()
    if "@" not in sender:
        return "no valid sender"
    if support_email and sender == support_email.lower():
        return "sent by ourselves"
    if str(msg.get("Auto-Submitted", "no")).strip().lower() != "no":
        return "automatic message (Auto-Submitted)"
    if msg.get("X-Autoreply") or msg.get("X-Autorespond") or msg.get("X-Auto-Response-Suppress"):
        return "automatic reply"
    if str(msg.get("Precedence", "")).strip().lower() in ("bulk", "list", "junk", "auto_reply"):
        return "bulk mail"
    if msg.get("List-Id") or msg.get("List-Unsubscribe"):
        return "mailing list or newsletter"
    if msg.get_content_type() == "multipart/report":
        return "delivery report (bounce)"
    if NO_REPLY.search(sender):
        return "no-reply sender"
    return None


def extract_body(msg):
    """Plain text of the email, without quoted history, trimmed to a safe size."""
    part = msg.get_body(preferencelist=("plain", "html"))
    if part is None:
        return ""
    try:
        text = part.get_content()
    except Exception:
        return ""
    if part.get_content_type() == "text/html":
        text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
        text = re.sub(r"(?i)<br\s*/?>|</p>|</div>", "\n", text)
        text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    lines = []
    for line in text.replace("\r", "").split("\n"):
        if QUOTE_START.match(line.strip()) or line.strip() == "--":
            break  # everything after is old quoted mail or a signature
        if line.lstrip().startswith(">"):
            continue
        lines.append(line.rstrip())
    body = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    return body[:MAX_BODY_CHARS]


def message_key(msg, raw):
    """A stable id for de-duplication: the Message-ID, or a hash if it is missing."""
    mid = clean_line(str(msg.get("Message-ID", "")))
    return mid or "hash-" + hashlib.sha256(raw).hexdigest()[:24]


def build_ticket(msg, raw, ticket_id):
    """Return (ticket, None) or (None, reason_it_was_not_a_ticket)."""
    name, addr = parseaddr(msg.get("From", ""))
    addr = clean_line(addr).lower()
    subject = clean_line(str(msg.get("Subject", "")))[:MAX_SUBJECT_CHARS]
    body = extract_body(msg)
    if not subject and not body:
        return None, "empty email"
    customer_name = clean_line(name) or addr.split("@")[0]
    text = f"{subject}\n\n{body}".strip() if subject else body
    ticket = {
        "ticket_id": ticket_id,
        "message": text,
        "channel": "email",
        "source": "email",
        "customer_email": addr,
        "customer_name": customer_name,
        "subject": subject,
        "message_id": message_key(msg, raw),
        "references": clean_line(str(msg.get("References", ""))),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    return ticket, None