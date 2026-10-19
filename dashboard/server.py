"""
TriageIQ - Live dashboard server.

Reads classified tickets from the Kafka topic "tickets.triaged" and streams
them to the browser page (dashboard/index.html) in real time.

Run (real data from Kafka):
    python dashboard/server.py

Run (simulated tickets, no Kafka or Ollama needed, just to preview):
    python dashboard/server.py --demo

Then open  http://localhost:8050

Agents can Approve, Edit or Send to human on every ticket. Those decisions are
saved in dashboard/decisions.db (live mode) or kept in memory only (demo mode).
"""

import argparse
import asyncio
import atexit
import json
import os
import random
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from collections import deque
from urllib.parse import urlparse
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR.parent / "producer"))  # so demo mode can reuse make_ticket()
sys.path.insert(0, str(BASE_DIR.parent / "gateway"))   # email sending (Step 5c)
import mail_config as cfg  # noqa: E402
import mail_send  # noqa: E402
import notify_push as push  # noqa: E402
import auth as authmod  # noqa: E402

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "tickets.triaged"
PORT = 8050  # 8050 so it does not clash with RootLens

CATEGORIES = ["Billing", "Technical", "Delivery", "Account", "General"]
PRIORITIES = ["High", "Medium", "Low"]
MOODS = ["Angry", "Frustrated", "Neutral", "Happy"]

state = {"mode": "live", "seq": 0}
history = deque(maxlen=500)  # last 500 events, so a page refresh is not empty
subscribers = set()          # one queue per open browser tab
stop_event = threading.Event()

# ---------- agent decisions (Approve / Edit / Send to human) ----------
VALID_ACTIONS = {"approved", "edited", "manual", "escalated"}
MAX_REPLY_CHARS = 2000
db = None
db_lock = threading.Lock()
auth = None  # set in live mode: the sign-in system. None = no login (demo mode, local only)


def init_db(path):
    """Open (and create if needed) the small database that remembers agent decisions."""
    global db
    db = sqlite3.connect(str(path), check_same_thread=False)
    db.execute(
        """CREATE TABLE IF NOT EXISTS decisions (
               uid TEXT PRIMARY KEY,
               ticket_id TEXT,
               action TEXT NOT NULL,
               original_reply TEXT,
               final_reply TEXT,
               decided_at REAL NOT NULL,
               sent_at REAL,
               send_error TEXT)"""
    )
    # databases made by Step 5 do not have the two email columns yet: add them
    have = {r[1] for r in db.execute("PRAGMA table_info(decisions)")}
    for col, kind in (("sent_at", "REAL"), ("send_error", "TEXT")):
        if col not in have:
            db.execute(f"ALTER TABLE decisions ADD COLUMN {col} {kind}")
    # which tickets the employee was already emailed about (so a restart does not email again)
    db.execute("CREATE TABLE IF NOT EXISTS notified (uid TEXT PRIMARY KEY, at REAL)")
    db.execute("CREATE TABLE IF NOT EXISTS pushed (uid TEXT PRIMARY KEY, at REAL)")
    db.commit()


def save_decision(uid, ticket_id, action, original_reply, final_reply):
    """Store one decision (a newer decision on the same ticket replaces the older one)."""
    row = {"uid": uid, "ticket_id": ticket_id, "action": action,
           "original_reply": original_reply, "final_reply": final_reply,
           "decided_at": time.time()}
    with db_lock:
        db.execute(
            """INSERT OR REPLACE INTO decisions
               (uid, ticket_id, action, original_reply, final_reply, decided_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (row["uid"], row["ticket_id"], row["action"],
             row["original_reply"], row["final_reply"], row["decided_at"]),
        )
        db.commit()
    return row


def set_send_result(uid, sent_at, error):
    """Remember whether the customer email went out."""
    with db_lock:
        db.execute("UPDATE decisions SET sent_at=?, send_error=? WHERE uid=?", (sent_at, error, uid))
        db.commit()


def emailing_possible():
    """Real customer emails are only sent in live mode, with the mailbox configured."""
    return state["mode"] == "live" and mail_send.can_send()


def email_customer(event, text):
    """Send the approved reply and store the outcome. Returns (sent_at, error)."""
    if state["mode"] != "live":
        return None, None  # demo: nothing is ever sent
    if not mail_send.can_send():
        error = "Email is not set up (fill SUPPORT_EMAIL and SUPPORT_APP_PASSWORD in .env)"
    else:
        try:
            mail_send.send_reply_to_customer(event, text)
            set_send_result(event["uid"], time.time(), None)
            return time.time(), None
        except Exception as e:  # the agent sees the reason and can press Resend
            error = f"{type(e).__name__}: {e}"[:300]
    set_send_result(event["uid"], None, error)
    return None, error


def notify_once(table, uid, ready, send, done_text, fail_text):
    """Run one kind of notification for a ticket, at most once (a restart does not repeat it)."""
    if not ready():
        return
    with db_lock:
        cur = db.execute(f"INSERT OR IGNORE INTO {table} VALUES (?, ?)", (uid, time.time()))
        db.commit()
        if cur.rowcount == 0:
            return  # already done for this ticket
    try:
        send()
        print(done_text)
    except Exception as e:
        print(f"{fail_text}: {e}")
        with db_lock:  # forget it, so the next start tries again
            db.execute(f"DELETE FROM {table} WHERE uid=?", (uid,))
            db.commit()


def maybe_notify(event):
    """Tell the employee about a new email ticket: by email, and as a phone alert."""
    if event.get("source") != "email":
        return
    uid = event["uid"]
    with db_lock:
        if db.execute("SELECT 1 FROM decisions WHERE uid=?", (uid,)).fetchone():
            return  # an agent already handled it
    notify_once("notified", uid, mail_send.can_notify, lambda: mail_send.send_agent_notification(event),
                f"Emailed the employee about {event['id']}", f"Could not email the employee about {event['id']}")
    notify_once("pushed", uid, push.configured, lambda: push.send_ticket_push(event),
                f"Sent a phone alert about {event['id']}", f"Could not send the phone alert about {event['id']}")


def load_decisions(uids):
    """Return {uid: {uid, action, final_reply, decided_at}} for the given tickets."""
    wanted = set(uids)
    with db_lock:
        rows = db.execute(
            "SELECT uid, action, final_reply, decided_at, sent_at, send_error FROM decisions").fetchall()
    keys = ("uid", "action", "final_reply", "decided_at", "sent_at", "send_error")
    return {r[0]: dict(zip(keys, r)) for r in rows if r[0] in wanted}


def find_event(uid):
    """Find a ticket event by its unique id (newest first)."""
    for ev in reversed(history):
        if ev.get("uid") == uid:
            return ev
    return None


def normalize(ticket: dict, ts: float, demo: bool) -> dict:
    """Turn a triaged ticket into the small event the browser needs."""
    triage = ticket.get("triage", {})
    # The producer restarts at TKT-10001 every run, so the id alone is not unique.
    # id + creation time is.
    uid = f"{ticket.get('ticket_id')}|{ticket.get('created_at') or ts}"
    return {
        "uid": uid,
        "id": ticket.get("ticket_id"),
        "message": ticket.get("message", ""),
        "channel": ticket.get("channel"),
        "source": ticket.get("source", "fake"),
        "customer_email": ticket.get("customer_email"),
        "customer_name": ticket.get("customer_name"),
        "message_id": ticket.get("message_id"),
        "references": ticket.get("references"),
        "subject": ticket.get("subject"),
        "ts": ts,
        "ai": {k: triage.get(k) for k in ("category", "priority", "mood")},
        "truth": ticket.get("label"),
        "latency_s": triage.get("latency_s"),
        "reply": triage.get("reply"),
        "reply_sources": triage.get("reply_sources"),
        "reply_error": triage.get("reply_error"),
        "draft_latency_s": triage.get("draft_latency_s"),
        "needs_human": bool(triage.get("needs_human")),
        "demo": demo,
    }


def publish(event: dict):
    """Store the event and push it to every open browser tab."""
    state["seq"] += 1
    event["seq"] = state["seq"]
    history.append(event)
    for q in list(subscribers):
        q.put_nowait(("ticket", event))


def kafka_feed(loop):
    """Background thread: read tickets.triaged and publish each one."""
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": f"triageiq-dashboard-{uuid.uuid4().hex[:8]}",  # new group = replay history on start
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(f"LIVE mode: reading Kafka topic '{TOPIC}'")

    last_error = None
    while not stop_event.is_set():
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            text = str(msg.error())
            if text != last_error:  # print each distinct problem once
                print(f"Kafka says: {text}")
                last_error = text
            continue
        try:
            ticket = json.loads(msg.value())
        except (json.JSONDecodeError, TypeError):
            continue
        kafka_ms = msg.timestamp()[1]
        ts = kafka_ms / 1000 if kafka_ms and kafka_ms > 0 else time.time()
        event = normalize(ticket, ts, demo=False)
        loop.call_soon_threadsafe(publish, event)
        if event["source"] == "email":  # email the employee without slowing this loop down
            threading.Thread(target=maybe_notify, args=(event,), daemon=True).start()
    consumer.close()


def pick(truth_value, options, accuracy):
    """Demo helper: usually return the true value, sometimes a wrong one."""
    if random.random() < accuracy:
        return truth_value
    return random.choice([o for o in options if o != truth_value])


async def demo_feed():
    """Simulated tickets and results, only to preview the dashboard."""
    import ticket_producer

    print("DEMO mode: simulated tickets (not real model results)")
    past = json.loads((BASE_DIR.parent / "rag" / "past_tickets.json").read_text(encoding="utf-8"))
    n = 0
    while True:
        n += 1
        ticket = ticket_producer.make_ticket(n)
        truth = ticket["label"]
        triage = {
            "category": pick(truth["category"], CATEGORIES, 0.88),
            "priority": pick(truth["priority"], PRIORITIES, 0.80),
            "mood": pick(truth["mood"], MOODS, 0.72),
            "latency_s": round(max(1.5, random.gauss(4.5, 0.9)), 2),
            "needs_human": random.random() < 0.03,
        }
        if triage["needs_human"]:
            triage.update({"category": "Unknown", "priority": "High", "mood": "Neutral"})
        # simulated reply: reuse a past resolution of the same category
        match = random.choice([p for p in past if p["category"] == truth["category"]] or past)
        if random.random() < 0.92:
            triage["reply"] = f"Thank you for contacting us. {match['resolution']} Support Team"
            triage["reply_sources"] = [{
                "id": match["id"],
                "score": round(random.uniform(0.62, 0.88), 2),
                "question": match["question"],
            }]
            triage["draft_latency_s"] = round(max(3.0, random.gauss(10, 2)), 2)
        else:
            triage["reply"] = None
            triage["reply_sources"] = []
            triage["reply_error"] = "No similar past ticket found, a human should answer"
        ticket["triage"] = triage
        if random.random() < 0.25:  # pretend some tickets arrived by email
            ticket.update({
                "source": "email", "channel": "email",
                "customer_email": f"customer{n}@example.com",
                "customer_name": f"Customer {n}",
                "subject": ticket["message"][:50],
            })
        publish(normalize(ticket, time.time(), demo=True))
        await asyncio.sleep(random.uniform(0.6, 1.8))


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(":memory:" if state["mode"] == "demo" else BASE_DIR / "decisions.db")
    state["loop"] = asyncio.get_running_loop()
    task = None
    if state["mode"] == "demo":
        task = asyncio.create_task(demo_feed())
    else:
        loop = asyncio.get_running_loop()
        threading.Thread(target=kafka_feed, args=(loop,), daemon=True).start()
    yield
    stop_event.set()
    if task:
        task.cancel()


app = FastAPI(title="TriageIQ Live Dashboard", lifespan=lifespan)


class SecurityHeaders:
    """Adds browser protections to every response."""
    CSP = ("default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
           "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; "
           "img-src 'self' data:; connect-src 'self'; manifest-src 'self'; worker-src 'self'; "
           "frame-ancestors 'none'; base-uri 'none'; form-action 'self'")

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                for k, v in (("content-security-policy", self.CSP), ("x-content-type-options", "nosniff"),
                             ("x-frame-options", "DENY"), ("referrer-policy", "no-referrer")):
                    headers.append((k.encode(), v.encode()))
            await send(message)

        await self.app(scope, receive, send_wrapper)


app.add_middleware(SecurityHeaders)
STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
NO_STORE = {"Cache-Control": "no-store"}


def same_origin(request: Request) -> bool:
    """Block forms and scripts from other websites that try to act with a signed-in browser."""
    origin = request.headers.get("origin")
    if not origin:
        return True  # not a browser page (scripts, tests)
    allowed = {request.headers.get("host"), request.headers.get("x-forwarded-host")}
    if cfg.PUBLIC_URL:
        allowed.add(urlparse(cfg.PUBLIC_URL).netloc)
    return urlparse(origin).netloc in allowed


def require_user(request: Request):
    """Used by every private route: the person must be signed in."""
    if auth is None:
        return "local"
    user = auth.user_from_request(request)
    if not user:
        raise HTTPException(status_code=401, detail="Please sign in")
    if request.method not in SAFE_METHODS and not same_origin(request):
        raise HTTPException(status_code=403, detail="Request blocked")
    return user


def client_ip(request: Request) -> str:
    host = request.client.host if request.client else "unknown"
    if host in ("127.0.0.1", "::1"):
        return request.headers.get("cf-connecting-ip") or host
    return host


def safe_next(value) -> str:
    """Only allow going back to a page on this site."""
    if isinstance(value, str) and value.startswith("/") and not value.startswith("//") and "\\" not in value and len(value) < 300:
        return value
    return "/"


@app.exception_handler(HTTPException)
async def http_error(request: Request, exc: HTTPException):
    return JSONResponse({"error": exc.detail}, status_code=exc.status_code, headers=NO_STORE)


@app.get("/")
async def index(request: Request):
    if auth is not None and not auth.user_from_request(request):
        return RedirectResponse("/login", status_code=302)
    return FileResponse(BASE_DIR / "index.html", headers=NO_STORE)


@app.get("/login")
async def login_page(request: Request):
    if auth is None or auth.user_from_request(request):
        return RedirectResponse("/", status_code=302)
    return FileResponse(BASE_DIR / "login.html", headers=NO_STORE)


@app.post("/api/login")
async def login(request: Request):
    if auth is None:
        return {"ok": True, "next": "/"}
    if not request.headers.get("content-type", "").startswith("application/json") or not same_origin(request):
        return reject("Request blocked", 403)
    ip = client_ip(request)
    wait = auth.blocked_for(ip)
    if wait:
        minutes = max(1, round(wait / 60))
        return JSONResponse({"error": f"Too many wrong attempts. Try again in {minutes} minute{'s' if minutes != 1 else ''}."},
                            status_code=429, headers={"Retry-After": str(wait), **NO_STORE})
    try:
        body = await request.json()
    except Exception:
        return reject("Invalid request")
    if not isinstance(body, dict) or not auth.check_credentials(str(body.get("user", "")), str(body.get("password", ""))):
        auth.register_fail(ip)
        await asyncio.sleep(0.6)  # slows down guessing
        return JSONResponse({"error": "Incorrect username or password."}, status_code=401, headers=NO_STORE)
    auth.clear_fails(ip)
    resp = JSONResponse({"ok": True, "next": safe_next(body.get("next"))}, headers=NO_STORE)
    resp.set_cookie(authmod.COOKIE, auth.make_token(), max_age=authmod.SESSION_SECONDS, httponly=True,
                    samesite="lax", secure=request.url.scheme == "https", path="/")
    return resp


@app.post("/api/logout")
async def logout(request: Request):
    if not same_origin(request):
        return reject("Request blocked", 403)
    resp = JSONResponse({"ok": True}, headers=NO_STORE)
    resp.delete_cookie(authmod.COOKIE, path="/")
    return resp


@app.get("/api/me")
async def me(user: str = Depends(require_user)):
    return {"user": user, "name": cfg.DASHBOARD_NAME or user.capitalize(), "mode": state["mode"], "login": auth is not None}


@app.get("/api/health")
async def health(request: Request):
    if auth is not None and not auth.user_from_request(request):
        return {"app": "triageiq"}  # strangers learn nothing else
    return {"app": "triageiq", "mode": state["mode"], "events_in_memory": len(history), "viewers": len(subscribers),
            "email_send": mail_send.can_send(), "email_notify": mail_send.can_notify(),
            "login": auth is not None, "public_url": cfg.PUBLIC_URL or None,
            "push": {"configured": push.configured(), "topic": cfg.NTFY_TOPIC or None, "server": cfg.NTFY_SERVER}}


@app.post("/api/test-push")
async def test_push(user: str = Depends(require_user)):
    if not push.configured():
        return reject("Phone alerts are not set up")
    try:
        await asyncio.to_thread(push.send_test)
    except Exception as e:
        return reject(f"Could not reach the alert service: {e}", 502)
    return {"ok": True}


@app.get("/manifest.webmanifest")
async def manifest():
    return FileResponse(STATIC_DIR / "manifest.webmanifest", media_type="application/manifest+json")


@app.get("/sw.js")
async def service_worker():
    return FileResponse(STATIC_DIR / "sw.js", media_type="application/javascript",
                        headers={"Service-Worker-Allowed": "/", "Cache-Control": "no-cache"})


@app.get("/events")
async def events(request: Request, user: str = Depends(require_user)):
    """Server-Sent Events: a one-way live stream from server to browser."""
    q: asyncio.Queue = asyncio.Queue()

    async def stream():
        subscribers.add(q)
        try:
            init = {
                "mode": state["mode"],
                "events": list(history),
                "decisions": load_decisions(e["uid"] for e in history),
            }
            # some proxies (including Cloudflare's tunnel) hold back a stream until enough bytes arrive,
            # so send a little padding first to push the connection open right away
            yield ":" + " " * 2048 + "\n\n"
            yield f"event: init\ndata: {json.dumps(init)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    kind, payload = await asyncio.wait_for(q.get(), timeout=15)
                    if kind == "close":  # Ctrl+C pressed: end this stream so shutdown is clean
                        return
                    yield f"event: {kind}\ndata: {json.dumps(payload)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            subscribers.discard(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def reject(message: str, status: int = 400):
    return JSONResponse({"error": message}, status_code=status)


@app.post("/api/decision")
async def decide(request: Request, user: str = Depends(require_user)):
    """An agent's decision on one ticket: approved, edited, manual (own reply) or escalated."""
    if not request.headers.get("content-type", "").startswith("application/json"):
        return reject("Send JSON", 415)
    try:
        body = await request.json()
    except Exception:
        return reject("Invalid JSON")
    if not isinstance(body, dict):
        return reject("Invalid JSON")

    uid, action, text = body.get("uid"), body.get("action"), body.get("final_reply")
    if not isinstance(uid, str) or action not in VALID_ACTIONS:
        return reject("Unknown ticket or action")
    event = find_event(uid)
    if event is None:
        return reject("That ticket is no longer in memory. Refresh the page.", 404)

    previous = load_decisions([uid]).get(uid)
    if previous and previous.get("sent_at"):
        return reject("The reply was already emailed to the customer, so it cannot be changed", 409)

    original = event.get("reply")  # always taken from the server's copy, never from the browser

    if action in ("edited", "manual"):
        if not isinstance(text, str) or not text.strip():
            return reject("The reply cannot be empty")
        text = text.strip()
        if len(text) > MAX_REPLY_CHARS:
            return reject(f"The reply is too long (max {MAX_REPLY_CHARS} characters)")

    if action == "approved":
        if not original:
            return reject("There is no draft to approve")
        text = original
    elif action == "edited":
        if not original:
            return reject("There is no draft to edit. Use Write reply instead")
        if text == original.strip():  # saved without changing anything = plain approval
            action, text = "approved", original
    elif action == "manual":
        if original:
            return reject("A draft exists. Edit it instead")
    else:  # escalated
        text = None

    row = save_decision(uid, event["id"], action, original, text)
    public = {k: row[k] for k in ("uid", "action", "final_reply", "decided_at")}
    public["sent_at"] = public["send_error"] = None
    if action != "escalated" and event.get("source") == "email":
        # the agent approved: now (and only now) the customer gets the email
        public["sent_at"], public["send_error"] = await asyncio.to_thread(email_customer, event, text)
    for q in list(subscribers):  # update every open browser tab
        q.put_nowait(("decision", public))
    return public


@app.post("/api/resend")
async def resend(request: Request, user: str = Depends(require_user)):
    """Try again to email an approved reply that failed to send."""
    try:
        body = await request.json()
    except Exception:
        return reject("Invalid JSON")
    uid = body.get("uid") if isinstance(body, dict) else None
    event = find_event(uid) if isinstance(uid, str) else None
    dec = load_decisions([uid]).get(uid) if event else None
    if not dec or dec["action"] == "escalated" or not dec.get("final_reply"):
        return reject("Nothing to send for this ticket", 404)
    if dec.get("sent_at"):
        return reject("The reply was already emailed", 409)
    sent_at, error = await asyncio.to_thread(email_customer, event, dec["final_reply"])
    dec = dict(dec, sent_at=sent_at, send_error=error)
    for q in list(subscribers):
        q.put_nowait(("decision", dec))
    return dec


def port_is_free(port):
    """True if nothing is listening on this port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


def is_triageiq(port):
    """True only if the program on this port is an old TriageIQ dashboard (never kill anything else)."""
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=2) as r:
            return json.loads(r.read()).get("app") == "triageiq"
    except Exception:
        return False


def pids_on_port(port):
    """Process ids listening on this port."""
    pids = set()
    try:
        if os.name == "nt":
            out = subprocess.run(["netstat", "-ano", "-p", "TCP"], capture_output=True, text=True).stdout
            for line in out.splitlines():
                parts = line.split()
                if len(parts) >= 5 and parts[3] == "LISTENING" and parts[1].endswith(f":{port}"):
                    pids.add(int(parts[4]))
        else:
            out = subprocess.run(["lsof", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
                                 capture_output=True, text=True).stdout
            pids = {int(p) for p in out.split()}
    except Exception:
        pass
    return pids


def free_port(port):
    """If an old TriageIQ dashboard holds the port, stop it. Returns True when the port is usable."""
    if port_is_free(port):
        return True
    if not is_triageiq(port):
        print(f"Port {port} is used by another program (not TriageIQ), so I will not touch it.")
        print(f"Use another port:  python dashboard/server.py --port {port + 10}")
        return False
    print(f"An old TriageIQ dashboard is still running on port {port}. Stopping it ...")
    for pid in pids_on_port(port):
        if pid == os.getpid():
            continue
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True)
        else:
            os.kill(pid, signal.SIGTERM)
    for _ in range(20):  # wait up to 5 seconds for the port to be released
        if port_is_free(port):
            return True
        time.sleep(0.25)
    print(f"Could not free port {port}. Try:  python dashboard/server.py --port {port + 10}")
    return False


CLOUDFLARED_HELP = """Cloudflare Tunnel is not installed yet. In PowerShell run:

    winget install --id Cloudflare.cloudflared

Then CLOSE this terminal, open a new one, and start the dashboard again with --share."""
TUNNEL_URL = re.compile(r"https://(?!api\.)[a-z0-9-]+\.trycloudflare\.com")


def find_cloudflared():
    exe = shutil.which("cloudflared")
    if exe:
        return exe
    for base in (os.environ.get("ProgramFiles"), os.environ.get("ProgramFiles(x86)"), os.environ.get("LOCALAPPDATA")):
        for sub in ("cloudflared/cloudflared.exe", "Cloudflare/cloudflared.exe", "Microsoft/WinGet/Links/cloudflared.exe"):
            if base and (Path(base) / sub).exists():
                return str(Path(base) / sub)
    return None


def start_tunnel(port):
    """Open a secure public address (https://...trycloudflare.com) that leads to this dashboard."""
    exe = find_cloudflared()
    if not exe:
        print(CLOUDFLARED_HELP)
        return None
    proc = subprocess.Popen([exe, "tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{port}"],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1,
                            encoding="utf-8", errors="replace")

    def read_output():
        found = False
        for line in proc.stdout:
            match = TUNNEL_URL.search(line)
            if match and not found:
                found = True
                url = match.group(0)
                cfg.PUBLIC_URL = url
                state["public_url"] = url
                print("\n" + "=" * 62)
                print("  Open this on your phone:  " + url)
                print(f"  Sign in as '{cfg.DASHBOARD_USER}' with the password from your .env file")
                print("  (it can take about 10 seconds before the link works)")
                print("=" * 62 + "\n")
                if push.configured():
                    try:
                        push.send_online(url)
                        print("Sent the link to your phone through ntfy.")
                    except Exception as e:
                        print(f"Could not send the link through ntfy: {e}")
            elif not found and ("ERR" in line or "error" in line.lower()):
                print("cloudflared: " + line.strip()[:200])
        if not state.get("stopping"):
            print("The tunnel stopped. Restart the dashboard with --share to get a new link.")

    threading.Thread(target=read_output, daemon=True).start()
    atexit.register(lambda: proc.poll() is None and proc.terminate())
    return proc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TriageIQ live dashboard")
    parser.add_argument("--demo", action="store_true", help="use simulated tickets instead of Kafka")
    parser.add_argument("--share", action="store_true", help="open a secure public link so you can use it from your phone")
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()
    if args.share and args.demo:
        sys.exit("--share needs the real dashboard, not --demo (demo data is fake and has no login).")
    state["mode"] = "demo" if args.demo else "live"
    if not free_port(args.port):
        sys.exit(1)

    if not args.demo:
        created = cfg.ensure_dashboard_secrets()
        auth = authmod.Auth(cfg.DASHBOARD_USER, cfg.DASHBOARD_PASSWORD, BASE_DIR / ".session_secret")
        if created:
            print("\nFirst start: TriageIQ created your sign-in and phone-alert settings and saved them in .env")
            if "DASHBOARD_PASSWORD" in created:
                print(f"  Sign in with:  username '{cfg.DASHBOARD_USER}'   password '{created['DASHBOARD_PASSWORD']}'")
            if "NTFY_TOPIC" in created:
                print(f"  Phone alerts:  install the free 'ntfy' app and subscribe to the topic  {created['NTFY_TOPIC']}")
            print()
    print(f"Open http://localhost:{args.port}  (mode: {state['mode']})   Ctrl+C to stop")

    class Server(uvicorn.Server):
        def handle_exit(self, sig, frame):
            """Ctrl+C: first tell every open browser stream to finish, then shut down."""
            stop_event.set()
            state["stopping"] = True
            loop = state.get("loop")
            if loop is not None:
                for q in list(subscribers):
                    loop.call_soon_threadsafe(q.put_nowait, ("close", None))
            super().handle_exit(sig, frame)

    config = uvicorn.Config(app, host="127.0.0.1", port=args.port, log_level="warning",
                            timeout_graceful_shutdown=3)
    tunnel = start_tunnel(args.port) if args.share else None
    try:
        Server(config).run()
    except KeyboardInterrupt:
        pass  # normal Ctrl+C, not an error
    finally:
        state["stopping"] = True
        if tunnel and tunnel.poll() is None:
            tunnel.terminate()
    print("Dashboard stopped.")