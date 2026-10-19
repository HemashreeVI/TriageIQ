"""
TriageIQ - phone alerts through ntfy (https://ntfy.sh).

ntfy is a free notification service with an Android / iPhone app. TriageIQ posts
one short message per new email ticket; tapping the notification opens the
approval screen. Only the ticket number, topic and priority are sent: never the
customer's name, address or message, because ntfy.sh is a public third-party server.
"""

import json
import urllib.parse
import urllib.request

import mail_config as cfg


def configured():
    return bool(cfg.NTFY_TOPIC)


def _post(payload):
    req = urllib.request.Request(
        cfg.NTFY_SERVER,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        resp.read()


def ticket_link(uid):
    """Link that opens one ticket in the console, or None when TriageIQ is not shared."""
    if not cfg.PUBLIC_URL:
        return None
    return f"{cfg.PUBLIC_URL}/#ticket=" + urllib.parse.quote(uid, safe="")


def send_ticket_push(event):
    ai = event.get("ai") or {}
    high = ai.get("priority") == "High" or event.get("needs_human")
    link = ticket_link(event["uid"])
    payload = {
        "topic": cfg.NTFY_TOPIC,
        "title": "Urgent ticket to approve" if high else "New ticket to approve",
        "message": f"{ai.get('category', 'New')} ticket {event.get('id')}, {str(ai.get('priority', '')).lower()} priority."
                   + ("" if link else " Open TriageIQ on your computer to review it."),
        "priority": 4 if high else 3,
        "tags": ["rotating_light"] if high else ["incoming_envelope"],
    }
    if link:
        payload["click"] = link
        payload["actions"] = [{"action": "view", "label": "Review and approve", "url": link, "clear": True}]
    _post(payload)


def send_online(url):
    """Tell the phone that TriageIQ is online and where to open it."""
    _post({
        "topic": cfg.NTFY_TOPIC,
        "title": "TriageIQ is online",
        "message": "Tap to open the console on this phone.",
        "priority": 3,
        "tags": ["white_check_mark"],
        "click": url,
        "actions": [{"action": "view", "label": "Open TriageIQ", "url": url, "clear": True}],
    })


def send_test():
    """A test alert, so the person can check the phone is set up."""
    link = cfg.PUBLIC_URL or None
    payload = {
        "topic": cfg.NTFY_TOPIC,
        "title": "TriageIQ test alert",
        "message": "Phone alerts are working. Real alerts open the ticket that needs approval.",
        "priority": 3,
        "tags": ["bell"],
    }
    if link:
        payload["click"] = link
    _post(payload)