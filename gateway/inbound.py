"""
TriageIQ - Step 5b: the email gateway (inbox -> Kafka).

Watches the company's support Gmail inbox. Every new customer email becomes a
ticket on the Kafka topic "tickets", exactly like the fake tickets before, so
the classifier, the reply drafter and the dashboard work on it unchanged.

Run (from the project folder, venv active):
    python gateway/inbound.py --check    # test the Gmail login only, changes nothing
    python gateway/inbound.py --once     # fetch what is waiting now, then stop
    python gateway/inbound.py            # keep watching the inbox (Ctrl+C to stop)

Safety built in:
  * automatic mail, bounces, newsletters and our own messages are ignored
  * the same email is never turned into two tickets
  * one sender can create at most MAX_PER_SENDER_PER_HOUR tickets per hour
  * an email is marked "read" only after Kafka confirmed it got the ticket
"""

import argparse
import imaplib
import smtplib
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mail_config as cfg  # noqa: E402
import mail_parse  # noqa: E402

STATE_DB = Path(__file__).resolve().parent / "mail_state.db"
FIRST_TICKET_NUMBER = 20001  # fake tickets use 10001+, email tickets use 20001+


class MailState:
    """Small SQLite memory: which emails we handled, and how many each sender sent."""

    def __init__(self, path):
        self.db = sqlite3.connect(str(path))
        self.db.execute("""CREATE TABLE IF NOT EXISTS handled (
            message_id TEXT PRIMARY KEY, outcome TEXT, at REAL)""")
        self.db.execute("""CREATE TABLE IF NOT EXISTS tickets (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, message_id TEXT UNIQUE,
            ticket_id TEXT, sender TEXT, at REAL)""")
        self.db.commit()

    def already_handled(self, message_id):
        return self.db.execute("SELECT 1 FROM handled WHERE message_id=?", (message_id,)).fetchone() is not None

    def next_ticket_id(self):
        row = self.db.execute("SELECT COALESCE(MAX(seq), 0) FROM tickets").fetchone()
        return f"EML-{FIRST_TICKET_NUMBER + row[0]}"

    def sender_count_last_hour(self, sender):
        row = self.db.execute("SELECT COUNT(*) FROM tickets WHERE sender=? AND at>?",
                              (sender, time.time() - 3600)).fetchone()
        return row[0]

    def record_ticket(self, message_id, ticket_id, sender):
        self.db.execute("INSERT OR IGNORE INTO tickets(message_id, ticket_id, sender, at) VALUES (?,?,?,?)",
                        (message_id, ticket_id, sender, time.time()))
        self.record_handled(message_id, "ticket " + ticket_id)

    def record_handled(self, message_id, outcome):
        self.db.execute("INSERT OR REPLACE INTO handled VALUES (?,?,?)", (message_id, outcome, time.time()))
        self.db.commit()


def make_producer():
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": cfg.KAFKA_BOOTSTRAP, "socket.timeout.ms": 5000})
    producer.list_topics(timeout=5)  # raises if Kafka is not reachable
    return producer


def send_to_kafka(producer, ticket):
    """Send one ticket and wait for Kafka's confirmation. True only if delivered."""
    import json
    result = {}

    def done(err, _msg):
        result["err"] = err

    producer.produce(cfg.TICKETS_TOPIC, key=ticket["ticket_id"], value=json.dumps(ticket), callback=done)
    producer.flush(10)
    return "err" in result and result["err"] is None


def handle_message(raw, producer, state):
    """Process one email. Returns (outcome, detail); outcome is 'ticket', 'skipped', 'duplicate' or 'failed'."""
    msg = mail_parse.parse_email(raw)
    key = mail_parse.message_key(msg, raw)
    if state.already_handled(key):
        return "duplicate", key

    reason = mail_parse.skip_reason(msg, cfg.SUPPORT_EMAIL)
    if reason:
        state.record_handled(key, "skipped: " + reason)
        return "skipped", reason

    ticket, reason = mail_parse.build_ticket(msg, raw, state.next_ticket_id())
    if ticket is None:
        state.record_handled(key, "skipped: " + reason)
        return "skipped", reason

    sender = ticket["customer_email"]
    if state.sender_count_last_hour(sender) >= cfg.MAX_PER_SENDER_PER_HOUR:
        state.record_handled(key, "skipped: rate limit")
        return "skipped", f"{sender} already sent {cfg.MAX_PER_SENDER_PER_HOUR} emails this hour"

    if not send_to_kafka(producer, ticket):
        return "failed", "Kafka did not confirm the ticket (will retry)"
    state.record_ticket(key, ticket["ticket_id"], sender)
    return "ticket", ticket


def connect_imap():
    imap = imaplib.IMAP4_SSL(cfg.IMAP_HOST, cfg.IMAP_PORT)
    imap.login(cfg.SUPPORT_EMAIL, cfg.SUPPORT_APP_PASSWORD)
    return imap


def poll_once(imap, producer, state):
    """Look at unread mail once. Returns how many tickets were created."""
    imap.select("INBOX")
    status, data = imap.uid("SEARCH", None, "UNSEEN")
    uids = data[0].split()[: cfg.MAX_PER_POLL]
    created = 0
    for uid in uids:
        status, parts = imap.uid("FETCH", uid, "(BODY.PEEK[])")  # PEEK: does not mark as read yet
        raw = next((p[1] for p in parts if isinstance(p, tuple)), None)
        if raw is None:
            continue
        outcome, detail = handle_message(raw, producer, state)
        if outcome == "ticket":
            created += 1
            print(f"New ticket {detail['ticket_id']} from {detail['customer_email']}: {detail['subject'][:60]!r}")
        elif outcome == "skipped":
            print(f"Ignored an email ({detail})")
        elif outcome == "failed":
            print(f"Could not deliver: {detail}")
            continue  # leave it unread, the next poll tries again
        imap.uid("STORE", uid, "+FLAGS", "(\\Seen)")  # only now mark it read
    return created


def check():
    """Test everything the gateway needs, change nothing."""
    missing = cfg.missing_settings()
    if missing:
        print("Missing in .env: " + ", ".join(missing))
        print("Copy .env.example to .env and fill it in (see the README).")
        return 1
    ok = True
    print(f"Support mailbox: {cfg.SUPPORT_EMAIL}")
    try:
        imap = connect_imap()
        imap.select("INBOX", readonly=True)
        _, data = imap.uid("SEARCH", None, "UNSEEN")
        print(f"[OK] Reading mail works ({len(data[0].split())} unread emails waiting)")
        imap.logout()
    except Exception as e:
        ok = False
        print(f"[FAIL] Reading mail: {e}")
        print("       Usually: wrong app password, or 2-Step Verification is not on for this Google account.")
    try:
        with smtplib.SMTP_SSL(cfg.SMTP_HOST, cfg.SMTP_PORT, timeout=15) as smtp:
            smtp.login(cfg.SUPPORT_EMAIL, cfg.SUPPORT_APP_PASSWORD)
        print("[OK] Sending mail works (needed for the next step)")
    except Exception as e:
        ok = False
        print(f"[FAIL] Sending mail: {e}")
    try:
        make_producer()
        print("[OK] Kafka is reachable")
    except Exception as e:
        ok = False
        print(f"[FAIL] Kafka: {e}  (is 'docker compose up -d' running?)")
    return 0 if ok else 1


def main():
    parser = argparse.ArgumentParser(description="Turn emails in the support inbox into tickets")
    parser.add_argument("--check", action="store_true", help="test the login only")
    parser.add_argument("--once", action="store_true", help="fetch what is waiting, then stop")
    args = parser.parse_args()

    if args.check:
        sys.exit(check())
    missing = cfg.missing_settings()
    if missing:
        print("Missing in .env: " + ", ".join(missing) + "  (run with --check for help)")
        sys.exit(1)

    state = MailState(STATE_DB)
    try:
        producer = make_producer()
    except Exception as e:
        print(f"Kafka is not reachable: {e}\nStart it with:  docker compose up -d")
        sys.exit(1)

    print(f"Watching {cfg.SUPPORT_EMAIL} every {cfg.POLL_SECONDS}s ... Ctrl+C to stop")
    imap = None
    try:
        while True:
            try:
                if imap is None:
                    imap = connect_imap()
                created = poll_once(imap, producer, state)
                if args.once:
                    print(f"Done. {created} new ticket(s).")
                    break
            except (imaplib.IMAP4.error, OSError) as e:
                print(f"Mail connection problem ({e}). Retrying in {cfg.POLL_SECONDS}s")
                imap = None
                if args.once:
                    sys.exit(1)
            time.sleep(cfg.POLL_SECONDS)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        try:
            if imap:
                imap.logout()
        except Exception:
            pass


if __name__ == "__main__":
    main()