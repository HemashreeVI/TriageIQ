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
import json
import random
import sqlite3
import sys
import threading
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR.parent / "producer"))  # so demo mode can reuse make_ticket()

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
               decided_at REAL NOT NULL)"""
    )
    db.commit()


def save_decision(uid, ticket_id, action, original_reply, final_reply):
    """Store one decision (a newer decision on the same ticket replaces the older one)."""
    row = {"uid": uid, "ticket_id": ticket_id, "action": action,
           "original_reply": original_reply, "final_reply": final_reply,
           "decided_at": time.time()}
    with db_lock:
        db.execute(
            "INSERT OR REPLACE INTO decisions VALUES (?, ?, ?, ?, ?, ?)",
            (row["uid"], row["ticket_id"], row["action"],
             row["original_reply"], row["final_reply"], row["decided_at"]),
        )
        db.commit()
    return row


def load_decisions(uids):
    """Return {uid: {uid, action, final_reply, decided_at}} for the given tickets."""
    wanted = set(uids)
    with db_lock:
        rows = db.execute("SELECT uid, action, final_reply, decided_at FROM decisions").fetchall()
    keys = ("uid", "action", "final_reply", "decided_at")
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
        loop.call_soon_threadsafe(publish, normalize(ticket, ts, demo=False))
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
        publish(normalize(ticket, time.time(), demo=True))
        await asyncio.sleep(random.uniform(0.6, 1.8))


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(":memory:" if state["mode"] == "demo" else BASE_DIR / "decisions.db")
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


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/api/health")
async def health():
    return {"mode": state["mode"], "events_in_memory": len(history), "viewers": len(subscribers)}


@app.get("/events")
async def events(request: Request):
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
            yield f"event: init\ndata: {json.dumps(init)}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    kind, payload = await asyncio.wait_for(q.get(), timeout=15)
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
async def decide(request: Request):
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
    for q in list(subscribers):  # update every open browser tab
        q.put_nowait(("decision", public))
    return public


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TriageIQ live dashboard")
    parser.add_argument("--demo", action="store_true", help="use simulated tickets instead of Kafka")
    parser.add_argument("--port", type=int, default=PORT)
    args = parser.parse_args()
    state["mode"] = "demo" if args.demo else "live"
    print(f"Open http://localhost:{args.port}  (mode: {state['mode']})")
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")