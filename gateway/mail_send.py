"""
TriageIQ - Step 5c: sending email.

Two kinds of mail leave the company mailbox:
  1. a notification to the EMPLOYEE (AGENT_EMAIL) with the AI's draft, asking them to review it
  2. the approved reply to the CUSTOMER, only after an agent clicked approve/save

Customer mail is never sent without that click.
"""

import re
import smtplib
from email.message import EmailMessage
from email.utils import formataddr, formatdate, make_msgid

import mail_config as cfg

EMAIL_RE = re.compile(r"^[^@\s,;<>\"']+@[^@\s,;<>\"']+\.[^@\s,;<>\"']+$")


def can_send():
    """Is the company mailbox configured (needed to reply to customers)?"""
    return not cfg.missing_settings()


def can_notify():
    """Is the employee address configured too (needed for the approval email)?"""
    return not cfg.missing_settings(need_agent=True)


def one_line(text):
    return re.sub(r"[\x00-\x1f\x7f]+", " ", text or "").strip()


def deliver(msg):
    """Send one message through the company mailbox. Raises on any problem."""
    with smtplib.SMTP_SSL(cfg.SMTP_HOST, cfg.SMTP_PORT, timeout=20) as smtp:
        smtp.login(cfg.SUPPORT_EMAIL, cfg.SUPPORT_APP_PASSWORD)
        smtp.send_message(msg)


def send_reply_to_customer(event, text):
    """Email the approved reply to whoever wrote the ticket, as a proper reply in their thread."""
    to = one_line(event.get("customer_email"))
    if not EMAIL_RE.match(to):
        raise ValueError("This ticket has no valid customer email address")
    if not text or not text.strip():
        raise ValueError("The reply is empty")
    subject = one_line(event.get("subject")) or "Your support request"
    if not subject.lower().startswith("re:"):
        subject = "Re: " + subject

    msg = EmailMessage()
    msg["From"] = formataddr((cfg.COMPANY_NAME, cfg.SUPPORT_EMAIL))
    msg["To"] = to
    msg["Subject"] = subject
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid(domain=cfg.SUPPORT_EMAIL.split("@")[-1])
    original_id = one_line(event.get("message_id"))
    if original_id.startswith("<"):  # keeps the reply inside the customer's conversation
        msg["In-Reply-To"] = original_id
        msg["References"] = (one_line(event.get("references")) + " " + original_id).strip()
    msg.set_content(text.strip() + "\n")
    deliver(msg)


def send_agent_notification(event):
    """Email the employee the AI's draft so they can review it in the dashboard."""
    ai = event.get("ai") or {}
    draft = event.get("reply")
    lines = [
        f"A new customer email needs your review.",
        "",
        f"Ticket:    {event.get('id')}",
        f"From:      {one_line(event.get('customer_name'))} <{one_line(event.get('customer_email'))}>",
        f"Subject:   {one_line(event.get('subject'))}",
        f"AI says:   {ai.get('category')} / {ai.get('priority')} priority / mood: {ai.get('mood')}",
        "",
        "--- What the customer wrote ---",
        (event.get("message") or "").strip(),
        "",
    ]
    if draft:
        lines += ["--- Draft reply written by the AI (NOT sent yet) ---", draft.strip(), ""]
    else:
        lines += ["--- No draft: the AI found no similar past ticket, so please write the reply yourself ---", ""]
    lines += [
        f"Review, edit and approve it here: {cfg.DASHBOARD_URL}",
        "The customer receives nothing until you approve it in the dashboard.",
        "(The link opens on the computer that runs TriageIQ.)",
    ]
    msg = EmailMessage()
    msg["From"] = formataddr(("TriageIQ", cfg.SUPPORT_EMAIL))
    msg["To"] = cfg.AGENT_EMAIL
    msg["Subject"] = one_line(
        f"[TriageIQ] Approve reply? {event.get('id')} · {ai.get('category')}/{ai.get('priority')} · {event.get('subject') or ''}"
    )[:200]
    msg["Date"] = formatdate(localtime=True)
    msg["Auto-Submitted"] = "auto-generated"  # our own gateway ignores mail marked like this
    msg.set_content("\n".join(lines) + "\n")
    deliver(msg)