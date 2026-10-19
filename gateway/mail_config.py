"""
TriageIQ - email settings.

Reads the file  .env  in the project folder (never committed to GitHub) and
exposes the settings the email gateway needs. No extra packages required.
"""

import os
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load_env(path=ROOT / ".env"):
    """Read KEY=VALUE lines from .env into the environment (real environment variables win)."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key.strip(), value)


load_env()

# the company's support mailbox (customers write to this address)
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "").strip()
# Gmail shows the app password in groups of four letters with spaces; the spaces are not part of it
SUPPORT_APP_PASSWORD = os.environ.get("SUPPORT_APP_PASSWORD", "").replace(" ", "").strip()
# the employee who approves replies (used in the next step)
AGENT_EMAIL = os.environ.get("AGENT_EMAIL", "").strip()

IMAP_HOST = os.environ.get("IMAP_HOST", "imap.gmail.com")
IMAP_PORT = int(os.environ.get("IMAP_PORT", "993"))
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))

# dashboard login (used when the console is opened from a phone through the tunnel)
DASHBOARD_USER = os.environ.get("DASHBOARD_USER", "agent").strip() or "agent"
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
DASHBOARD_NAME = os.environ.get("DASHBOARD_NAME", "").strip()  # shown in the greeting, e.g. Hema

# phone alerts through ntfy (free app). The topic name works like a secret: anyone who knows it can read the alerts.
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh").strip().rstrip("/")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()

PUBLIC_URL = ""  # set at runtime when the tunnel is up (the address changes every time)

COMPANY_NAME = os.environ.get("COMPANY_NAME", "TriageIQ Support")  # shown as the sender name
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "http://localhost:8050")  # link in the employee email

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "20"))
MAX_PER_SENDER_PER_HOUR = int(os.environ.get("MAX_PER_SENDER_PER_HOUR", "5"))
MAX_PER_POLL = 25  # never swallow a huge inbox in one go

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TICKETS_TOPIC = "tickets"


def missing_settings(need_agent=False):
    """Names of required settings that are empty."""
    missing = []
    if not SUPPORT_EMAIL:
        missing.append("SUPPORT_EMAIL")
    if not SUPPORT_APP_PASSWORD:
        missing.append("SUPPORT_APP_PASSWORD")
    if need_agent and not AGENT_EMAIL:
        missing.append("AGENT_EMAIL")
    return missing


def dashboard_url():
    """The address employees should open: the tunnel address when sharing, else the local one."""
    return PUBLIC_URL or DASHBOARD_URL


def ensure_dashboard_secrets(path=ROOT / ".env"):
    """Create a login password and a phone-alert topic the first time, and save them in .env.

    Returns the values that were just created (empty when everything already existed)."""
    global DASHBOARD_PASSWORD, NTFY_TOPIC
    alphabet = "abcdefghjkmnpqrstuvwxyz23456789"  # no look-alike characters (0/o, 1/l)
    created = {}
    if not DASHBOARD_PASSWORD:
        created["DASHBOARD_PASSWORD"] = "-".join("".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3))
    if not NTFY_TOPIC:
        created["NTFY_TOPIC"] = "triageiq-" + secrets.token_hex(8)
    if created:
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        if text and not text.endswith("\n"):
            text += "\n"
        text += "\n# Created automatically by TriageIQ\n" + "".join(f"{k}={v}\n" for k, v in created.items())
        path.write_text(text, encoding="utf-8")
        for k, v in created.items():
            os.environ[k] = v
        DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", DASHBOARD_PASSWORD)
        NTFY_TOPIC = os.environ.get("NTFY_TOPIC", NTFY_TOPIC)
    return created