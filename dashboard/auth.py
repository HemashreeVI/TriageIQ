"""
TriageIQ - sign-in for the console.

One shared account (DASHBOARD_USER / DASHBOARD_PASSWORD in .env). After a correct
sign-in the browser gets a signed cookie that lasts 14 days. The cookie cannot be
forged or edited, it dies when the password changes, and repeated wrong passwords
from one address are blocked for a few minutes.
"""

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from pathlib import Path

SESSION_SECONDS = 14 * 24 * 3600
COOKIE = "triageiq_session"
MAX_FAILS = 6          # wrong attempts allowed ...
WINDOW_SECONDS = 600   # ... within this many seconds, per address


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


class Auth:
    def __init__(self, user, password, secret_path):
        self.user = user
        self.password = password
        secret_path = Path(secret_path)
        if secret_path.exists():
            base = secret_path.read_bytes()
        else:
            base = secrets.token_bytes(32)
            secret_path.write_bytes(base)
            try:
                os.chmod(secret_path, 0o600)
            except OSError:
                pass
        # the key depends on the password, so changing the password signs everyone out
        self.key = hmac.new(base, password.encode(), hashlib.sha256).digest()
        self.fails = {}

    # ---- passwords ----
    def check_credentials(self, user, password):
        ok_user = hmac.compare_digest((user or "").strip().lower().encode(), self.user.lower().encode())
        ok_pass = hmac.compare_digest((password or "").encode(), self.password.encode())
        return ok_user and ok_pass   # both are always compared, so timing reveals nothing

    # ---- cookie ----
    def make_token(self, now=None):
        body = _b64(json.dumps({"u": self.user, "exp": int((now or time.time()) + SESSION_SECONDS)}).encode())
        sig = _b64(hmac.new(self.key, body.encode(), hashlib.sha256).digest())
        return f"{body}.{sig}"

    def read_token(self, token, now=None):
        try:
            body, sig = token.split(".", 1)
            good = _b64(hmac.new(self.key, body.encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(sig, good):
                return None
            data = json.loads(_unb64(body))
            if data.get("exp", 0) < (now or time.time()):
                return None
            return data.get("u")
        except Exception:
            return None

    def user_from_request(self, request):
        token = request.cookies.get(COOKIE)
        return self.read_token(token) if token else None

    # ---- brute-force protection ----
    def blocked_for(self, ip, now=None):
        """Seconds the address must still wait, or 0."""
        now = now or time.time()
        recent = [t for t in self.fails.get(ip, []) if now - t < WINDOW_SECONDS]
        self.fails[ip] = recent
        if len(recent) >= MAX_FAILS:
            return int(WINDOW_SECONDS - (now - recent[0])) + 1
        return 0

    def register_fail(self, ip, now=None):
        self.fails.setdefault(ip, []).append(now or time.time())

    def clear_fails(self, ip):
        self.fails.pop(ip, None)